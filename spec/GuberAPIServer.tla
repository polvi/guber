--------------------------- MODULE GuberAPIServer ---------------------------

EXTENDS Naturals, FiniteSets, TLC

(* 
  CONSTANTS represent the configuration of our model:
  - CRDNames: The set of possible CustomResourceDefinition names.
  - ResourceNames: The set of possible instance names for those CRDs.
  - Namespaces: The set of possible namespace names.
  - Schemas: Abstract representations of OpenAPI validation schemas.
  - Specs: Possible desired states (spec) for the resources.
  - MaxVersion: A limit to keep the state space finite for model checking.
*)
CONSTANTS
  CRDNames,
  ResourceNames,
  Namespaces,
  Schemas,
  Specs,
  MaxVersion

ASSUME
  /\ CRDNames # {}
  /\ ResourceNames # {}
  /\ Namespaces # {}
  /\ Schemas # {}
  /\ Specs # {}
  /\ MaxVersion \in Nat

(*
  VARIABLES represent the state of the API Server and the cluster:
  - crds: The set of registered CRDs.
  - schemaOf: A mapping from a CRD to its validation schema.
  - scopeOf: Whether a CRD is "Namespaced" or "Cluster".
  - namespaces: The set of active namespaces.
  - resources: The set of existing Custom Resource instances.
  - resourceCRD: Which CRD type a specific resource belongs to.
  - resourceNamespace: The namespace of the resource (or "None" for cluster-scoped).
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
  scopeOf,
  namespaces,
  resources,
  resourceCRD,
  resourceNamespace,
  resourceSpec,
  resourceVersion,
  resourceGeneration,
  resourceObservedGen,
  resourceStatus,
  resourceDeletionTimestamp,
  resourceFinalizers

vars ==
  << crds, schemaOf, scopeOf, namespaces, resources, resourceCRD, resourceNamespace, 
     resourceSpec, resourceVersion, resourceGeneration, resourceObservedGen, 
     resourceStatus, resourceDeletionTimestamp, resourceFinalizers >>

(* Abstract schema validation. *)
SchemaValid(spec, schema) ==
  TRUE

Init ==
  /\ crds = {}
  /\ schemaOf = [c \in CRDNames |-> "None"]
  /\ scopeOf = [c \in CRDNames |-> "None"]
  /\ namespaces = {"default"}
  /\ resources = {}
  /\ resourceCRD = [r \in ResourceNames |-> "None"]
  /\ resourceNamespace = [r \in ResourceNames |-> "None"]
  /\ resourceSpec = [r \in ResourceNames |-> "None"]
  /\ resourceVersion = [r \in ResourceNames |-> 0]
  /\ resourceGeneration = [r \in ResourceNames |-> 0]
  /\ resourceObservedGen = [r \in ResourceNames |-> 0]
  /\ resourceStatus = [r \in ResourceNames |-> "None"]
  /\ resourceDeletionTimestamp = [r \in ResourceNames |-> FALSE]
  /\ resourceFinalizers = [r \in ResourceNames |-> {}]

(* Models 'kubectl create namespace' *)
CreateNamespace ==
  \E ns \in Namespaces :
    /\ ns \notin namespaces
    /\ namespaces' = namespaces \cup { ns }
    /\ UNCHANGED << crds, schemaOf, scopeOf, resources, resourceCRD, resourceNamespace, 
                    resourceSpec, resourceVersion, resourceGeneration, 
                    resourceObservedGen, resourceStatus, resourceDeletionTimestamp, 
                    resourceFinalizers >>

(* Models 'kubectl delete namespace' with cascading deletion. *)
DeleteNamespace ==
  \E ns \in namespaces :
    /\ ns # "default"
    /\ LET remaining == { r \in resources : resourceNamespace[r] # ns } IN
       /\ namespaces' = namespaces \ { ns }
       /\ resources' = remaining
       /\ resourceCRD' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceCRD[r] ELSE "None" ]
       /\ resourceNamespace' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceNamespace[r] ELSE "None" ]
       /\ resourceSpec' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceSpec[r] ELSE "None" ]
       /\ resourceVersion' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceVersion[r] ELSE 0 ]
       /\ resourceGeneration' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceGeneration[r] ELSE 0 ]
       /\ resourceObservedGen' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceObservedGen[r] ELSE 0 ]
       /\ resourceStatus' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceStatus[r] ELSE "None" ]
       /\ resourceDeletionTimestamp' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceDeletionTimestamp[r] ELSE FALSE ]
       /\ resourceFinalizers' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceFinalizers[r] ELSE {} ]
       /\ UNCHANGED << crds, schemaOf, scopeOf >>

(* Models 'kubectl apply -f crd.yaml' *)
CreateCRD ==
  \E c \in CRDNames, s \in Schemas, scope \in {"Namespaced", "Cluster"} :
    /\ c \notin crds
    /\ crds' = crds \cup { c }
    /\ schemaOf' = [ schemaOf EXCEPT ![c] = s ]
    /\ scopeOf' = [ scopeOf EXCEPT ![c] = scope ]
    /\ UNCHANGED << namespaces, resources, resourceCRD, resourceNamespace, resourceSpec, 
                    resourceVersion, resourceGeneration, resourceObservedGen, 
                    resourceStatus, resourceDeletionTimestamp, resourceFinalizers >>

(* Models cascading deletion of resources when a CRD is removed. *)
DeleteCRD ==
  \E c \in crds :
    LET remaining == { r \in resources : resourceCRD[r] # c } IN
    /\ crds' = crds \ { c }
    /\ schemaOf' = [ schemaOf EXCEPT ![c] = "None" ]
    /\ scopeOf' = [ scopeOf EXCEPT ![c] = "None" ]
    /\ resources' = remaining
    /\ resourceCRD' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceCRD[r] ELSE "None" ]
    /\ resourceNamespace' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceNamespace[r] ELSE "None" ]
    /\ resourceSpec' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceSpec[r] ELSE "None" ]
    /\ resourceVersion' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceVersion[r] ELSE 0 ]
    /\ resourceGeneration' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceGeneration[r] ELSE 0 ]
    /\ resourceObservedGen' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceObservedGen[r] ELSE 0 ]
    /\ resourceStatus' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceStatus[r] ELSE "None" ]
    /\ resourceDeletionTimestamp' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceDeletionTimestamp[r] ELSE FALSE ]
    /\ resourceFinalizers' = [ r \in ResourceNames |-> IF r \in remaining THEN resourceFinalizers[r] ELSE {} ]
    /\ UNCHANGED << namespaces >>

(* Models creation of a resource. *)
CreateResource ==
  \E r \in ResourceNames, c \in crds, spec \in Specs :
    /\ r \notin resources
    /\ SchemaValid(spec, schemaOf[c])
    /\ \E ns \in (Namespaces \cup {"None"}) :
        /\ IF scopeOf[c] = "Namespaced" 
           THEN ns \in namespaces 
           ELSE ns = "None"
        /\ resourceNamespace' = [ resourceNamespace EXCEPT ![r] = ns ]
        /\ resources' = resources \cup { r }
        /\ resourceCRD' = [ resourceCRD EXCEPT ![r] = c ]
        /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = spec ]
        /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = 1 ]
        /\ resourceGeneration' = [ resourceGeneration EXCEPT ![r] = 1 ]
        /\ resourceObservedGen' = [ resourceObservedGen EXCEPT ![r] = 0 ]
        /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "Initial" ]
        /\ resourceDeletionTimestamp' = [ resourceDeletionTimestamp EXCEPT ![r] = FALSE ]
        /\ resourceFinalizers' = [ resourceFinalizers EXCEPT ![r] = {"guber-controller"} ]
        /\ UNCHANGED << crds, schemaOf, scopeOf, namespaces >>

(* Models 'kubectl apply' with optimistic concurrency control. *)
UpdateResource ==
  \E r \in resources, spec \in Specs :
    /\ resourceDeletionTimestamp[r] = FALSE
    /\ resourceVersion[r] < MaxVersion
    /\ SchemaValid(spec, schemaOf[resourceCRD[r]])
    /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = spec ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = resourceVersion[r] + 1 ]
    /\ resourceGeneration' = [ resourceGeneration EXCEPT ![r] = resourceGeneration[r] + 1 ]
    /\ UNCHANGED << crds, schemaOf, scopeOf, namespaces, resources, resourceCRD, 
                    resourceNamespace, resourceObservedGen, resourceStatus, 
                    resourceDeletionTimestamp, resourceFinalizers >>

(* Models the Controller updating the status subresource. *)
ReconcileResource(r) ==
    /\ r \in resources
    /\ resourceObservedGen[r] < resourceGeneration[r]
    /\ resourceDeletionTimestamp[r] = FALSE
    /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "Ready" ]
    /\ resourceObservedGen' = [ resourceObservedGen EXCEPT ![r] = resourceGeneration[r] ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = IF resourceVersion[r] < MaxVersion 
                                                          THEN resourceVersion[r] + 1 
                                                          ELSE resourceVersion[r] ]
    /\ UNCHANGED << crds, schemaOf, scopeOf, namespaces, resources, resourceCRD, 
                    resourceNamespace, resourceSpec, resourceGeneration, 
                    resourceDeletionTimestamp, resourceFinalizers >>

(* Models the Controller cleaning up and removing finalizers. *)
FinalizeResource(r) ==
    /\ r \in resources
    /\ resourceDeletionTimestamp[r] = TRUE
    /\ "guber-controller" \in resourceFinalizers[r]
    /\ resourceFinalizers' = [ resourceFinalizers EXCEPT ![r] = resourceFinalizers[r] \ {"guber-controller"} ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = IF resourceVersion[r] < MaxVersion 
                                                          THEN resourceVersion[r] + 1 
                                                          ELSE resourceVersion[r] ]
    /\ UNCHANGED << crds, schemaOf, scopeOf, namespaces, resources, resourceCRD, 
                    resourceNamespace, resourceSpec, resourceGeneration, 
                    resourceObservedGen, resourceStatus, resourceDeletionTimestamp >>

(* Models 'kubectl delete' initiation. *)
RequestDeleteResource(r) ==
    /\ r \in resources
    /\ resourceDeletionTimestamp[r] = FALSE
    /\ resourceDeletionTimestamp' = [ resourceDeletionTimestamp EXCEPT ![r] = TRUE ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = IF resourceVersion[r] < MaxVersion 
                                                          THEN resourceVersion[r] + 1 
                                                          ELSE resourceVersion[r] ]
    /\ UNCHANGED << crds, schemaOf, scopeOf, namespaces, resources, resourceCRD, 
                    resourceNamespace, resourceSpec, resourceGeneration, 
                    resourceObservedGen, resourceStatus, resourceFinalizers >>

(* Models the API server removing the resource once finalizers are gone. *)
ObserveGarbageCollection(r) ==
    /\ r \in resources
    /\ resourceDeletionTimestamp[r] = TRUE
    /\ resourceFinalizers[r] = {}
    /\ resources' = resources \ { r }
    /\ resourceCRD' = [ resourceCRD EXCEPT ![r] = "None" ]
    /\ resourceNamespace' = [ resourceNamespace EXCEPT ![r] = "None" ]
    /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = "None" ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = 0 ]
    /\ resourceGeneration' = [ resourceGeneration EXCEPT ![r] = 0 ]
    /\ resourceObservedGen' = [ resourceObservedGen EXCEPT ![r] = 0 ]
    /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "None" ]
    /\ resourceDeletionTimestamp' = [ resourceDeletionTimestamp EXCEPT ![r] = FALSE ]
    /\ resourceFinalizers' = [ resourceFinalizers EXCEPT ![r] = {} ]
    /\ UNCHANGED << crds, schemaOf, scopeOf, namespaces >>

Next ==
  \/ CreateNamespace
  \/ DeleteNamespace
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

NamespaceCorrectness == 
  \forall r \in resources : 
    IF scopeOf[resourceCRD[r]] = "Namespaced"
    THEN resourceNamespace[r] \in namespaces
    ELSE resourceNamespace[r] = "None"

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
