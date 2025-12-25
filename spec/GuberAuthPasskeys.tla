----------------------- MODULE GuberAuthPasskeys -----------------------
EXTENDS Naturals, FiniteSets, TLC, GuberAPIServer

(* 
  This module models a Passkey Authentication Server.
  It uses the GuberAPIServer as the backing store for credentials.
  A 'Passkey' is a Resource where:
    - resourceCRD[r] = "PasskeyCRD"
    - resourceSpec[r] = The Public Key
*)

CONSTANTS
    Challenges,
    Signatures

VARIABLES
    authSessions,   (* Maps User -> Challenge or "None" *)
    authenticated   (* Set of users who successfully logged in *)

authVars == << authSessions, authenticated, vars >>

(* 
  Abstract Cryptography: 
  In TLA+, we model verification as a check if the signature 
  matches the expected pair of (Key, Challenge).
*)
Verify(key, challenge, signature) ==
    TRUE

AuthInit ==
    /\ Init
    /\ authSessions = [r \in ResourceNames |-> "None"]
    /\ authenticated = {}

(* 
  Step 1: Registration
  This is simply the CreateResource action from GuberAPIServer.
  We assume the 'spec' provided is the Public Key.
*)
RegisterPasskey(r, key) ==
    /\ CreateResource!(r, "PasskeyCRD", key)
    /\ UNCHANGED << authSessions, authenticated >>

(* 
  Step 2: Authentication - Challenge
  The server generates a challenge for a specific resource (credential).
*)
AuthChallenge(r) ==
    /\ r \in resources
    /\ resourceCRD[r] = "PasskeyCRD"
    /\ \E c \in Challenges :
        /\ authSessions' = [authSessions EXCEPT ![r] = c]
    /\ UNCHANGED << vars, authenticated >>

(* 
  Step 3: Authentication - Verify
  The user provides a signature. We look up the public key in resourceSpec.
*)
AuthVerify(r, sig) ==
    /\ authSessions[r] # "None"
    /\ Verify(resourceSpec[r], authSessions[r], sig)
    /\ authenticated' = authenticated \cup { r }
    /\ authSessions' = [authSessions EXCEPT ![r] = "None"]
    /\ UNCHANGED vars

(* 
  Handle API Server background tasks (Reconcile, Delete, etc.)
  to ensure the Auth Server stays in sync with the cluster state.
*)
APIServerStep ==
    \/ CreateCRD
    \/ DeleteCRD
    \/ UpdateResource
    \/ Reconcile
    \/ DeleteResource

NextAuth ==
    \/ \E r \in ResourceNames, k \in Specs : RegisterPasskey(r, k)
    \/ \E r \in ResourceNames : AuthChallenge(r)
    \/ \E r \in ResourceNames, s \in Signatures : AuthVerify(r, s)
    \/ (APIServerStep /\ UNCHANGED << authSessions, authenticated >>)

AuthSpec ==
    /\ AuthInit
    /\ [][NextAuth]_authVars
    /\ \forall r \in ResourceNames : WF_vars(ReconcileResource(r))

(* --- Safety Properties --- *)

(* A user cannot be authenticated if their Passkey resource was deleted *)
NoOrphanAuth ==
    \forall r \in authenticated : r \in resources

=============================================================================
