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
    /\ schemaOf' = [ schemaOf EXCEPT ![c] = s ]
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
    /\ resourceCRD' = [ resourceCRD EXCEPT ![r] = c ]
    /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = spec ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = 1 ]
    /\ UNCHANGED << crds, schemaOf >>

(*
Update a resource with optimistic concurrency
*)
UpdateResource ==
  \E r \in resources, spec \in Specs, expected \in Nat :
    /\ expected = resourceVersion[r]
    /\ SchemaValid(spec, schemaOf[resourceCRD[r]])
    /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = spec ]
    /\ resourceVersion' =
        [ resourceVersion EXCEPT ![r] = resourceVersion[r] + 1 ]
    /\ UNCHANGED << crds, schemaOf, resources, resourceCRD >>

(*
Delete a resource with optimistic concurrency
*)
DeleteResource ==
  \E r \in resources, expected \in Nat :
    /\ expected = resourceVersion[r]
    /\ resources' = resources \ { r }
    /\ resourceCRD' =
        [ x \in (DOMAIN resourceCRD) \ { r } |-> resourceCRD[x] ]
    /\ resourceSpec' =
        [ x \in (DOMAIN resourc]()
