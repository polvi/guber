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
  MaxVersion

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
  - resourceVersion: The 'metadata.resourceVersion' for optimistic concurrency.
  - resourceGeneration: Incremented on spec changes.
  - resourceObservedGen: The generation last seen by the controller.
  - resourceStatus: The 'status' (observed state) of a resource.
  - resourceDeletionTimestamp: Boolean flag indicating deletion is in progress.
  - resourceFinalizers: Set of strings preventing hard deletion.
*)
VARIABLES
  crds,
  schemaOf,
  resources,
  resourceCRD,
  resourceSpec,
  resourceVersion,
  resourceGeneration,
  resourceObservedGen,
  resourceStatus,
  resourceDeletionTimestamp,
  resourceFinalizers

vars ==
  << crds, schemaOf, resources, resourceCRD, resourceSpec, resourceVersion, 
     resourceGeneration, resourceObservedGen, resourceStatus, 
     resourceDeletionTimestamp, resourceFinalizers >>

(* Abstract schema validation. *)
SchemaValid(spec, schema) ==
  TRUE

Init ==
  /\ crds = {}
  /\ schemaOf = [c \in CRDNames |-> "None"]
  /\ resources = {}
  /\ resourceCRD = [r \in ResourceNames |-> "None"]
  /\ resourceSpec = [r \in ResourceNames |-> "None"]
  /\ resourceVersion = [r \in ResourceNames |-> 0]
  /\ resourceGeneration = [r \in ResourceNames |-> 0]
  /\ resourceObservedGen = [r \in ResourceNames |-> 0]
  /\ resourceStatus = [r \in ResourceNames |-> "None"]
  /\ resourceDeletionTimestamp = [r \in ResourceNames |-> FALSE]
  /\ resourceFinalizers = [r \in ResourceNames |-> {}]

(* Models 'kubectl apply -f crd.yaml' *)
CreateCRD ==
  \E c \in CRDNames, s \in Schemas :
    /\ c \notin crds
    /\ crds' = crds \cup { c }
    /\ schemaOf' = [ schemaOf EXCEPT ![c] = s ]
    /\ UNCHANGED << resources, resourceCRD, resourceSpec, resourceVersion, 
                    resourceGeneration, resourceObservedGen, resourceStatus, 
                    resourceDeletionTimestamp, resourceFinalizers >>

(* Models cascading deletion of resources when a CRD is removed. *)
DeleteCRD ==
  \E c \in crds :
    LET remaining == { r \in resources : resourceCRD[r] # c } IN
    /\ crds' = crds \ { c }
    /\ schemaOf' = [ schemaOf EXCEPT ![c] = "None" ]
    /\ resources' = remaining
    /\ resourceCRD' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceCRD[r] ELSE "None" ]
    /\ resourceSpec' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceSpec[r] ELSE "None" ]
    /\ resourceVersion' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceVersion[r] ELSE 0 ]
    /\ resourceGeneration' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceGeneration[r] ELSE 0 ]
    /\ resourceObservedGen' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceObservedGen[r] ELSE 0 ]
    /\ resourceStatus' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceStatus[r] ELSE "None" ]
    /\ resourceDeletionTimestamp' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceDeletionTimestamp[r] ELSE FALSE ]
    /\ resourceFinalizers' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceFinalizers[r] ELSE {} ]

(* Models creation of a resource with an initial finalizer. *)
CreateResource ==
  \E r \in ResourceNames, c \in crds, spec \in Specs :
    /\ r \notin resources
    /\ SchemaValid(spec, schemaOf[c])
    /\ resources' = resources \cup { r }
    /\ resourceCRD' = [ resourceCRD EXCEPT ![r] = c ]
    /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = spec ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = 1 ]
    /\ resourceGeneration' = [ resourceGeneration EXCEPT ![r] = 1 ]
    /\ resourceObservedGen' = [ resourceObservedGen EXCEPT ![r] = 0 ]
    /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "Initial" ]
    /\ resourceDeletionTimestamp' = [ resourceDeletionTimestamp EXCEPT ![r] = FALSE ]
    /\ resourceFinalizers' = [ resourceFinalizers EXCEPT ![r] = {"guber-controller"} ]
    /\ UNCHANGED << crds, schemaOf >>

(* Models 'kubectl apply' with optimistic concurrency control. *)
UpdateResource ==
  \E r \in resources, spec \in Specs :
    /\ resourceDeletionTimestamp[r] = FALSE
    /\ resourceVersion[r] < MaxVersion
    /\ SchemaValid(spec, schemaOf[resourceCRD[r]])
    /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = spec ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = resourceVersion[r] + 1 ]
    /\ resourceGeneration' = [ resourceGeneration EXCEPT ![r] = resourceGeneration[r] + 1 ]
    /\ UNCHANGED << crds, schemaOf, resources, resourceCRD, resourceObservedGen, 
                    resourceStatus, resourceDeletionTimestamp, resourceFinalizers >>

(* Models the Controller updating the status subresource. *)
ReconcileResource(r) ==
    /\ r \in resources
    /\ resourceObservedGen[r] < resourceGeneration[r]
    /\ resourceDeletionTimestamp[r] = FALSE
    /\ resourceVersion[r] < MaxVersion
    /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "Ready" ]
    /\ resourceObservedGen' = [ resourceObservedGen EXCEPT ![r] = resourceGeneration[r] ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = resourceVersion[r] + 1 ]
    /\ UNCHANGED << crds, schemaOf, resources, resourceCRD, resourceSpec, 
                    resourceGeneration, resourceDeletionTimestamp, resourceFinalizers >>

(* Models the Controller cleaning up and removing finalizers. *)
FinalizeResource(r) ==
    /\ r \in resources
    /\ resourceDeletionTimestamp[r] = TRUE
    /\ "guber-controller" \in resourceFinalizers[r]
    /\ resourceVersion[r] < MaxVersion
    /\ resourceFinalizers' = [ resourceFinalizers EXCEPT ![r] = resourceFinalizers[r] \ {"guber-controller"} ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = resourceVersion[r] + 1 ]
    /\ UNCHANGED << crds, schemaOf, resources, resourceCRD, resourceSpec, 
                    resourceGeneration, resourceObservedGen, resourceStatus, 
                    resourceDeletionTimestamp >>

(* Models 'kubectl delete' initiation. *)
RequestDeleteResource(r) ==
    /\ r \in resources
    /\ resourceDeletionTimestamp[r] = FALSE
    /\ resourceVersion[r] < MaxVersion
    /\ resourceDeletionTimestamp' = [ resourceDeletionTimestamp EXCEPT ![r] = TRUE ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = resourceVersion[r] + 1 ]
    /\ UNCHANGED << crds, schemaOf, resources, resourceCRD, resourceSpec, 
                    resourceGeneration, resourceObservedGen, resourceStatus, 
                    resourceFinalizers >>

(* Models the API server removing the resource once finalizers are gone. *)
ObserveGarbageCollection(r) ==
    /\ r \in resources
    /\ resourceDeletionTimestamp[r] = TRUE
    /\ resourceFinalizers[r] = {}
    /\ resources' = resources \ { r }
    /\ resourceCRD' = [ resourceCRD EXCEPT ![r] = "None" ]
    /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = "None" ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = 0 ]
    /\ resourceGeneration' = [ resourceGeneration EXCEPT ![r] = 0 ]
    /\ resourceObservedGen' = [ resourceObservedGen EXCEPT ![r] = 0 ]
    /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "None" ]
    /\ resourceDeletionTimestamp' = [ resourceDeletionTimestamp EXCEPT ![r] = FALSE ]
    /\ resourceFinalizers' = [ resourceFinalizers EXCEPT ![r] = {} ]
    /\ UNCHANGED << crds, schemaOf >>

Next ==
  \/ CreateCRD
  \/ DeleteCRD
  \/ CreateResource
  \/ UpdateResource
  \/ \E r \in ResourceNames : ReconcileResource(r)
  \/ \E r \in ResourceNames : FinalizeResource(r)
  \/ \E r \in ResourceNames : RequestDeleteResource(r)
  \/ \E r \in ResourceNames : ObserveGarbageCollection(r)

Spec == 
  /\ Init 
  /\ [][Next]_vars 
  /\ \forall r \in ResourceNames : WF_vars(ReconcileResource(r))
  /\ \forall r \in ResourceNames : WF_vars(FinalizeResource(r))
  /\ \forall r \in ResourceNames : WF_vars(ObserveGarbageCollection(r))

(* --- Invariants --- *)

ValidResourceCRD == \forall r \in resources : resourceCRD[r] \in crds
SchemaCorrectness == \forall c \in crds : schemaOf[c] # "None"
VersionWellFormed == \forall r \in resources : resourceVersion[r] >= 1

(* --- Liveness --- *)

(* If a spec changes, the controller eventually observes that generation or the resource is deleted. *)
EventuallyConsistent ==
  \forall r \in ResourceNames : 
    []((r \in resources /\ resourceObservedGen[r] < resourceGeneration[r] /\ resourceDeletionTimestamp[r] = FALSE) 
        => <>(resourceObservedGen[r] = resourceGeneration[r] \/ r \notin resources \/ resourceDeletionTimestamp[r] = TRUE))

(* If a resource is marked for deletion, it eventually leaves the system. *)
EventuallyDeleted ==
  \forall r \in ResourceNames :
    [](resourceDeletionTimestamp[r] => <>(r \notin resources))

=============================================================================
