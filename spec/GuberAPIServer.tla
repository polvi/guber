--------------------------- MODULE GuberAPIServer ---------------------------

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  CRDNames,
  ResourceNames,
  Schemas,
  Specs,
  MaxVersion,
  SymmetrySet

ASSUME
  /\ CRDNames # {}
  /\ ResourceNames # {}
  /\ Schemas # {}
  /\ Specs # {}
  /\ MaxVersion \in Nat

VARIABLES
  crds,
  schemaOf,
  resources,
  resourceCRD,
  resourceSpec,
  resourceVersion,
  resourceStatus

vars ==
  << crds,
     schemaOf,
     resources,
     resourceCRD,
     resourceSpec,
     resourceVersion,
     resourceStatus >>

(*
Abstract schema validation
*)
SchemaValid(spec, schema) ==
  TRUE

(*
Initial state
*)
Init ==
  /\ crds = {}
  /\ schemaOf = [c \in CRDNames |-> "None"]
  /\ resources = {}
  /\ resourceCRD = [r \in ResourceNames |-> "None"]
  /\ resourceSpec = [r \in ResourceNames |-> "None"]
  /\ resourceVersion = [r \in ResourceNames |-> 0]
  /\ resourceStatus = [r \in ResourceNames |-> "None"]

(*
Create a CRD
*)
CreateCRD ==
  \E c \in CRDNames, s \in Schemas :
    /\ c \notin crds
    /\ crds' = crds \cup { c }
    /\ schemaOf' = [ schemaOf EXCEPT ![c] = s ]
    /\ UNCHANGED << resources, resourceCRD, resourceSpec, resourceVersion, resourceStatus >>

(*
Delete a CRD and cascade delete resources
*)
DeleteCRD ==
  \E c \in crds :
    LET remaining == { r \in resources : resourceCRD[r] # c } IN
    /\ crds' = crds \ { c }
    /\ schemaOf' = [ schemaOf EXCEPT ![c] = "None" ]
    /\ resources' = remaining
    /\ resourceCRD' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceCRD[r] ELSE "None" ]
    /\ resourceSpec' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceSpec[r] ELSE "None" ]
    /\ resourceVersion' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceVersion[r] ELSE 0 ]
    /\ resourceStatus' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceStatus[r] ELSE "None" ]

(*
Create a resource
*)
CreateResource ==
  \E r \in ResourceNames, c \in crds, spec \in Specs :
    /\ r \notin resources
    /\ SchemaValid(spec, schemaOf[c])
    /\ resources' = resources \cup { r }
    /\ resourceCRD' = [ resourceCRD EXCEPT ![r] = c ]
    /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = spec ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = 1 ]
    /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "Pending" ]
    /\ UNCHANGED << crds, schemaOf >>

(*
Update a resource
*)
UpdateResource ==
  \E r \in resources, spec \in Specs :
    LET expected == resourceVersion[r] IN
    /\ expected < MaxVersion
    /\ SchemaValid(spec, schemaOf[resourceCRD[r]])
    /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = spec ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = expected + 1 ]
    /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "Pending" ]
    /\ UNCHANGED << crds, schemaOf, resources, resourceCRD >>

(*
Reconcile
*)
Reconcile ==
  \E r \in resources :
    /\ resourceStatus[r] = "Pending"
    /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "Ready" ]
    /\ UNCHANGED << crds, schemaOf, resources, resourceCRD, resourceSpec, resourceVersion >>

(*
Delete a resource
*)
DeleteResource ==
  \E r \in resources :
    /\ resources' = resources \ { r }
    /\ resourceCRD' = [ resourceCRD EXCEPT ![r] = "None" ]
    /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = "None" ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = 0 ]
    /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "None" ]
    /\ UNCHANGED << crds, schemaOf >>

Next ==
  \/ CreateCRD
  \/ DeleteCRD
  \/ CreateResource
  \/ UpdateResource
  \/ Reconcile
  \/ DeleteResource

Spec == Init /\ [][Next]_vars /\ WF_vars(Reconcile)

(* Invariants *)

ValidResourceCRD ==
  \forall r \in resources : resourceCRD[r] \in crds

SchemaCorrectness ==
  \forall c \in crds : schemaOf[c] # "None"

VersionWellFormed ==
  \forall r \in resources : resourceVersion[r] >= 1

(* Liveness *)
EventuallyConsistent ==
  \forall r \in ResourceNames : 
    []((r \in resources /\ resourceStatus[r] = "Pending") => <>(resourceStatus[r] = "Ready" \/ r \notin resources))

=============================================================================
