package core
import("context";"encoding/json";"testing";artifact "everything-go/contracts/task-api/artifacts-v3";"everything-go/internal/session")
func TestQACurrentContinuationSealReadableThroughOriginalArtifactPort(t *testing.T){
 h,p,c,intent,r:=extensionFormal(t);ctx:=context.Background()
 oldSeal:=correctionSeal(t,h,r,false)
 f:=h.exec.(*extensionExec); originalSend:=f.onSend
 f.onSend=func(s *session.Session,req,content string){if req!=r.ChildRequestID{f.final="round2 exact e\u0301 🧪"};originalSend(s,req,content)}
 request:=r2OwnerRequest(t,"qa-current-round",continuityInput(r,p,h,"original same-task continuation"));raw,_:=json.Marshal(request);c.CallID="qa-current-round"
 appendResponse:=h.ExecuteTask(ctx,c,raw);if !appendResponse.OK{t.Fatal("setup same-task append",appendResponse.Error)}
 latest,e:=h.delegations.LatestRound(ctx,r.ID);if e!=nil||latest.Version!=2{t.Fatal(latest,e)}
 newRound:=continuityWait(t,h,latest.ID,"terminal");newSeal:=correctionSeal(t,h,newRound,false)
 if newSeal.Hash==oldSeal.Hash||newRound.ChildSessionID!=r.ChildSessionID{t.Fatal("fixture lacks changed same-task result")}
 read:=func(seal,hash,corr string){raw,_:=json.Marshal(map[string]any{"type":"task_api_request","api_version":artifact.Version,"correlation_id":corr,"operation":"read_artifact_bytes","input":map[string]any{"task_id":intent.TaskID,"seal_id":seal,"artifact_id":"result.txt","expected_hash":hash,"offset":0,"limit":32768}});c.CallID=corr;response:=h.ExecuteTask(ctx,c,raw);t.Logf("task=%s current-round=%s current-request=%s requested-seal=%s response=%+v",intent.TaskID,newRound.ID,newRound.ChildRequestID,seal,response);if corr=="qa-new-seal"&&!response.OK{t.Fatal("same canonical task current continuation seal cannot be read",response.Error)}}
 read(oldSeal.ID,oldSeal.Hash,"qa-old-seal")
 read(newSeal.ID,newSeal.Hash,"qa-new-seal")
}
