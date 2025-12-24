--------------------------- MODULE GuberAPIServer ---------------------------

EXTENDS Naturals, FiniteSets

CONSTANTS
  CRDNames,
  ResourceNames,
  Schemas,
  Specs

ASSUME
  CRDNames # {} /\
  ResourceNames # {} /\
  Schemas # {} /\
  Specs # {}

VARIABLES
  crds,
  schemaOf,
  resources,
  resourceCRD,
  resourceSpec,
  resourceVersion

vars ==
  << crds,
     schemaOf,
     resources,
     resourceCRD,
     resourceSpec,
     resourceVersion >>

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
  /\ schemaOf = [ c \in {} |-> CHOOSE s : TRUE ]
  /\ resources = {}
  /\ resourceCRD = [ r \in {} |-> CHOOSE c : TRUE ]
  /\ resourceSpec = [ r \in {} |-> CHOOSE s : TRUE ]
  /\ resourceVersion = [ r \in {} |-> 0 ]

(*
Create a CRD
*)
CreateCRD ==
  \E c \in CRDNames, s \in Schemas :
    /\ c \notin crds
    /\ crds' = crds \cup { c }
    /\ schemaOf' = [ d \in DOMAIN schemaOf \cup {c} |-> IF d = c THEN s ELSE schemaOf[d] ]
    /\ UNCHANGED << resources, resourceCRD, resourceSpec, resourceVersion >>

(*
Delete a CRD and cascade delete resources
*)
DeleteCRD ==
  \E c \in crds :
    LET remaining ==
          { r \in resources : resourceCRD[r] # c }
    IN
    /\ crds' = crds \ { c }
    /\ schemaOf' =
        [ d \in (DOMAIN schemaOf) \ { c } |-> schemaOf[d] ]
    /\ resources' = remaining
    /\ resourceCRD' =
        [ r \in remaining |-> resourceCRD[r] ]
    /\ resourceSpec' =
        [ r \in remaining |-> resourceSpec[r] ]
    /\ resourceVersion' =
        [ r \in remaining |-> resourceVersion[r] ]

(*
Create a resource, version starts at 1
*)
CreateResource ==
  \E r \in ResourceNames, c \in crds, spec \in Specs :
    /\ r \notin resources
    /\ SchemaValid(spec, schemaOf[c])
    /\ resources' = resources \cup { r }
    /\ resourceCRD' = [ s \in DOMAIN resourceCRD \cup {r} |-> IF s = r THEN c ELSE resourceCRD[s] ]
    /\ resourceSpec' = [ s \in DOMAIN resourceSpec \cup {r} |-> IF s = r THEN spec ELSE resourceSpec[s] ]
    /\ resourceVersion' = [ s \in DOMAIN resourceVersion \cup {r} |-> IF s = r THEN 1 ELSE resourceVersion[s] ]
    /\ UNCHANGED << crds, schemaOf >>

(*
Update a resource with optimistic concurrency
*)
UpdateResource ==
  \E r \in resources, spec \in Specs :
    LET expected == resourceVersion[r] IN
    /\ SchemaValid(spec, schemaOf[resourceCRD[r]])
    /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = spec ]
    /\ resourceVersion' =
        [ resourceVersion EXCEPT ![r] = expected + 1 ]
    /\ UNCHANGED << crds, schemaOf, resources, resourceCRD >>

(*
Delete a resource
*)
DeleteResource ==
  \E r \in resources :
    /\ resources' = resources \ { r }
    /\ resourceCRD' = [ x \in (DOMAIN resourceCRD) \ { r } |-> resourceCRD[x] ]
    /\ resourceSpec' = [ x \in (DOMAIN resourceSpec) \ { r } |-> resourceSpec[x] ]
    /\ resourceVersion' = [ x \in (DOMAIN resourceVersion) \ { r } |-> resourceVersion[x] ]
    /\ UNCHANGED << crds, schemaOf >>

Next ==
  \/ CreateCRD
  \/ DeleteCRD
  \/ CreateResource
  \/ UpdateResource
  \/ DeleteResource

Spec == Init /\ [][Next]_vars

(* Invariants *)

ValidResourceCRD ==
  \forall r \in resources : resourceCRD[r] \in crds

SchemaCorrectness ==
  DOMAIN schemaOf = crds

VersionWellFormed ==
  \forall r \in resources : resourceVersion[r] > 0

=============================================================================
