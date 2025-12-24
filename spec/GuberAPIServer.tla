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
  resourceStatus  \* Added to track reconciliation state

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
  /\ schemaOf = [c \in {} |-> <<>>]
  /\ resources = {}
  /\ resourceCRD = [r \in {} |-> <<>>]
  /\ resourceSpec = [r \in {} |-> <<>>]
  /\ resourceVersion = [r \in {} |-> 0]
  /\ resourceStatus = [r \in {} |-> ""]

(*
Create a CRD
*)
CreateCRD ==
  \E c \in CRDNames, s \in Schemas :
    /\ c \notin crds
    /\ crds' = crds \cup { c }
    /\ schemaOf' = [ d \in DOMAIN schemaOf \cup {c} |-> IF d = c THEN s ELSE schemaOf[d] ]
    /\ UNCHANGED << resources, resourceCRD, resourceSpec, resourceVersion, resourceStatus >>

(*
Delete a CRD and cascade delete resources
*)
DeleteCRD ==
  \E c \in crds :
    LET remaining == { r \in resources : resourceCRD[r] # c } IN
    /\ crds' = crds \ { c }
    /\ schemaOf' = [ d \in (DOMAIN schemaOf) \ { c } |-> schemaOf[d] ]
    /\ resources' = remaining
    /\ resourceCRD' = [ r \in remaining |-> resourceCRD[r] ]
    /\ resourceSpec' = [ r \in remaining |-> resourceSpec[r] ]
    /\ resourceVersion' = [ r \in remaining |-> resourceVersion[r] ]
    /\ resourceStatus' = [ r \in remaining |-> resourceStatus[r] ]

(*
Create a resource, version starts at 1. 
Status starts as "Pending" to represent intent before reconciliation.
*)
CreateResource ==
  \E r \in ResourceNames, c \in crds, spec \in Specs :
    /\ r \notin resources
    /\ SchemaValid(spec, schemaOf[c])
    /\ resources' = resources \cup { r }
    /\ resourceCRD' = [ s \in DOMAIN resourceCRD \cup {r} |-> IF s = r THEN c ELSE resourceCRD[s] ]
    /\ resourceSpec' = [ s \in DOMAIN resourceSpec \cup {r} |-> IF s = r THEN spec ELSE resourceSpec[s] ]
    /\ resourceVersion' = [ s \in DOMAIN resourceVersion \cup {r} |-> IF s = r THEN 1 ELSE resourceVersion[s] ]
    /\ resourceStatus' = [ s \in DOMAIN resourceStatus \cup {r} |-> IF s = r THEN "Pending" ELSE resourceStatus[s] ]
    /\ UNCHANGED << crds, schemaOf >>

(*
Update a resource with optimistic concurrency.
Resets status to "Pending" because the controller needs to re-apply changes.
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
Reconcile: The controller observes the "Pending" state and provisions infrastructure.
This models the "bun db:init" or "scheduled handler" logic from the README.
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
    /\ resourceCRD' = [ x \in (DOMAIN resourceCRD) \ { r } |-> resourceCRD[x] ]
    /\ resourceSpec' = [ x \in (DOMAIN resourceSpec) \ { r } |-> resourceSpec[x] ]
    /\ resourceVersion' = [ x \in (DOMAIN resourceVersion) \ { r } |-> resourceVersion[x] ]
    /\ resourceStatus' = [ x \in (DOMAIN resourceStatus) \ { r } |-> resourceStatus[x] ]
    /\ UNCHANGED << crds, schemaOf >>

Next ==
  \/ CreateCRD
  \/ DeleteCRD
  \/ CreateResource
  \/ UpdateResource
  \/ Reconcile
  \/ DeleteResource

Spec == Init /\ [][Next]_vars /\ WF_vars(Reconcile)

(* Symmetry definition for TLC *)
Symmetry == Permutations(SymmetrySet)

(* Invariants *)

ValidResourceCRD ==
  \forall r \in resources : resourceCRD[r] \in crds

SchemaCorrectness ==
  DOMAIN schemaOf = crds

VersionWellFormed ==
  \forall r \in resources : resourceVersion[r] >= 0

(* Liveness: Resources should eventually be reconciled if updates stop *)
EventuallyConsistent ==
  \forall r \in ResourceNames : 
    []((r \in resources /\ resourceStatus[r] = "Pending") => <>(resourceStatus[r] = "Ready" \/ r \notin resources))

=============================================================================
