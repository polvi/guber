----------------------- MODULE GuberAuthPasskeys -----------------------
EXTENDS Naturals, FiniteSets, TLC, GuberAPIServer

(* 
  This module models a Passkey Authentication Server.
  It uses the GuberAPIServer as the backing store for credentials.
*)

CONSTANTS
    Users,          (* Set of human user identities *)
    Challenges,     (* Set of possible random challenges *)
    Signatures      (* Set of valid cryptographic signatures *)

VARIABLES
    authSessions,   (* Maps Resource -> Challenge or "None" *)
    usedChallenges, (* Set of challenges already consumed *)
    authenticated,  (* Set of Users currently logged in *)
    resourceUser    (* Maps Resource -> User *)

authVars == << authSessions, usedChallenges, authenticated, resourceUser, vars >>

(* 
  Refined Cryptography: 
  A signature is valid only if it was generated for the specific 
  combination of the Public Key and the Challenge.
  We model this as a function/record check.
*)
IsValidSignature(sig, key, challenge) ==
    /\ sig.key = key
    /\ sig.challenge = challenge

AuthInit ==
    /\ Init
    /\ authSessions = [r \in ResourceNames |-> "None"]
    /\ usedChallenges = {}
    /\ authenticated = {}
    /\ resourceUser = [r \in ResourceNames |-> "None"]

(* 
  Step 1: Registration
  A user registers a new Passkey. We store the User ID in our local mapping
  and the Public Key in the API Server's resourceSpec.
*)
RegisterPasskey(r, u, key) ==
    /\ "PasskeyCRD" \in crds
    /\ r \notin resources
    /\ u \in Users
    /\ SchemaValid(key, schemaOf["PasskeyCRD"])
    /\ resources' = resources \cup { r }
    /\ resourceCRD' = [ resourceCRD EXCEPT ![r] = "PasskeyCRD" ]
    /\ resourceSpec' = [ resourceSpec EXCEPT ![r] = key ]
    /\ resourceVersion' = [ resourceVersion EXCEPT ![r] = 1 ]
    /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "Pending" ]
    /\ resourceUser' = [ resourceUser EXCEPT ![r] = u ]
    /\ UNCHANGED << crds, schemaOf, authSessions, usedChallenges, authenticated >>

(* 
  Step 2: Authentication - Challenge
  The server generates a challenge. It must not have been used before.
*)
AuthChallenge(r) ==
    /\ r \in resources
    /\ resourceStatus[r] = "Ready" (* Pitfall fix: Must be reconciled *)
    /\ \E c \in Challenges \ usedChallenges :
        /\ authSessions' = [authSessions EXCEPT ![r] = c]
    /\ UNCHANGED << vars, usedChallenges, authenticated, resourceUser >>

(* 
  Step 3: Authentication - Verify
  The user provides a signature. 
  We verify the signature against the stored key and the active challenge.
*)
AuthVerify(r, sig) ==
    /\ authSessions[r] # "None"
    /\ IsValidSignature(sig, resourceSpec[r], authSessions[r])
    /\ authenticated' = authenticated \cup { resourceUser[r] }
    /\ usedChallenges' = usedChallenges \cup { authSessions[r] }
    /\ authSessions' = [authSessions EXCEPT ![r] = "None"]
    /\ UNCHANGED << vars, resourceUser >>

(* 
  Handle API Server background tasks.
  If a resource is deleted, we must clean up our local user mapping.
*)
APIServerStep ==
    \/ CreateCRD
    \/ DeleteCRD
    \/ UpdateResource
    \/ Reconcile
    \/ DeleteResource

NextAuth ==
    \/ \E r \in ResourceNames, u \in Users, k \in Specs : RegisterPasskey(r, u, k)
    \/ \E r \in ResourceNames : AuthChallenge(r)
    \/ \E r \in ResourceNames, s \in Signatures : AuthVerify(r, s)
    \/ /\ APIServerStep 
       /\ resourceUser' = [ r \in ResourceNames |-> IF r \in resources' THEN resourceUser[r] ELSE "None" ]
       /\ UNCHANGED << authSessions, usedChallenges, authenticated >>

AuthSpec ==
    /\ AuthInit
    /\ [][NextAuth]_authVars
    /\ \forall r \in ResourceNames : WF_vars(ReconcileResource(r))

(* --- Safety Properties --- *)

(* A user cannot be authenticated if they have no Ready passkeys in the system *)
NoOrphanAuth ==
    \forall u \in authenticated : 
        \E r \in resources : 
            /\ resourceUser[r] = u 
            /\ resourceStatus[r] = "Ready"

(* Replay Protection: A challenge in authSessions cannot be in usedChallenges *)
NoChallengeReplay ==
    \forall r \in ResourceNames :
        authSessions[r] \notin usedChallenges

=============================================================================
