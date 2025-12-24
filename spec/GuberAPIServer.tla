--------------------------- MODULE GuberAPIServer ---------------------------

EXTENDS Naturals, FiniteSets, TLC

(* 
  CONSTANTS represent the configuration of our model:
  - CRDNames: The set of possible CustomResourceDefinition names.
  - ResourceNames: The set of possible instance names for those CRDs.
  - Schemas: Abstract representations of OpenAPI validation schemas.
  - Specs: Possible desired states (spec) for the resources.
  - MaxVersion: A limit to keep the state space finite for model checking.
*)
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

(*
  VARIABLES represent the state of the API Server and the cluster:
  - crds: The set of registered CRDs.
  - schemaOf: A mapping from a CRD to its validation schema.
  - resources: The set of existing Custom Resource instances.
  - resourceCRD: Which CRD type a specific resource belongs to.
  - resourceSpec: The 'spec' (desired state) of a resource.
  - resourceVersion: The 'metadata.resourceVersion' for concurrency control.
  - resourceStatus: The 'status' (observed state) of a resource.
*)
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
  Abstract schema validation. 
  In a real K8s API server, this would be an OpenAPI v3 check.
*)
SchemaValid(spec, schema) ==
  TRUE

(*
  Initial state: The cluster starts empty with no CRDs or resources.
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
  CreateCRD: Models 'kubectl apply -f crd.yaml'.
  Registers a new type in the API server.
*)
CreateCRD ==
  \E c \in CRDNames, s \in Schemas :
    /\ c \notin crds
    /\ crds' = crds \cup { c }
    /\ schemaOf' = [ schemaOf EXCEPT ![c] = s ]
    /\ UNCHANGED << resources, resourceCRD, resourceSpec, resourceVersion, resourceStatus >>

(*
  DeleteCRD: Models the deletion of a CRD.
  Kubernetes performs cascading deletion: when a CRD is removed, 
  all its Custom Resources are also deleted.
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
  CreateResource: Models 'kubectl apply -f resource.yaml' for a new object.
  The API server validates the spec against the CRD schema and sets initial status to Pending.
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
  UpdateResource: Models 'kubectl edit' or 'kubectl apply' on an existing object.
  Increments resourceVersion and moves status back to Pending so the controller 
  knows it needs to reconcile the new desired state.
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
  ReconcileResource: Models the Controller's Reconcile() function.
  If a resource is Pending (Desired != Observed), the controller acts 
  to make it Ready.
*)
ReconcileResource(r) ==
    /\ r \in resources
    /\ resourceStatus[r] = "Pending"
    /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "Ready" ]
    /\ UNCHANGED << crds, schemaOf, resources, resourceCRD, resourceSpec, resourceVersion >>

(*
  Reconcile: The non-deterministic trigger of the reconciliation loop.
*)
Reconcile ==
  \E r \in ResourceNames : ReconcileResource(r)

(*
  DeleteResource: Models 'kubectl delete'.
  Removes the instance from the API server.
*)
DeleteResource ==
  \E r \in resources :
    /\ resources' = resources \ { r }
    /\ resourceCRD' = [ resourceCRD EXCEPT ![r] = "None" ]
    /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = "None" ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = 0 ]
    /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "None" ]
    /\ UNCHANGED << crds, schemaOf >>

(*
  Next: The set of all possible atomic transitions in the system.
*)
Next ==
  \/ CreateCRD
  \/ DeleteCRD
  \/ CreateResource
  \/ UpdateResource
  \/ Reconcile
  \/ DeleteResource

(* 
  Spec: The complete system specification.
  Includes Weak Fairness (WF) on ReconcileResource. This models the 
  guarantee that the controller is running and will eventually process 
  any resource that needs reconciliation.
*)
Spec == 
  /\ Init 
  /\ [][Next]_vars 
  /\ \forall r \in ResourceNames : WF_vars(ReconcileResource(r))

(* --- Invariants (Safety Properties) --- *)

(* Every existing resource must belong to a CRD that is currently registered. *)
ValidResourceCRD ==
  \forall r \in resources : resourceCRD[r] \in crds

(* Every registered CRD must have an associated schema. *)
SchemaCorrectness ==
  \forall c \in crds : schemaOf[c] # "None"

(* Every existing resource must have a version of at least 1. *)
VersionWellFormed ==
  \forall r \in resources : resourceVersion[r] >= 1

(* --- Liveness Properties --- *)

(* 
  EventuallyConsistent: The core promise of the Kubernetes model.
  If a resource is in a Pending state, it will eventually reach Ready, 
  unless it is deleted from the system first.
*)
EventuallyConsistent ==
  \forall r \in ResourceNames : 
    []((r \in resources /\ resourceStatus[r] = "Pending") => <>(resourceStatus[r] = "Ready" \/ r \notin resources))

=============================================================================
