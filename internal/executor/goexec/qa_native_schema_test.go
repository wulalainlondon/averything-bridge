package goexec
import("testing";a4 "everything-go/contracts/task-api/artifacts-v4")
func TestQAArtifactCombinedToolsAcceptedByProductionMCP(t *testing.T){
 defs,e:=a4.CompatibleToolInputs();if e!=nil{t.Fatal(e)}
 for name,definition:=range defs{t.Run(name,func(t *testing.T){schema,e:=mcpTaskInputSchema(definition);if e!=nil{t.Fatal("generated advertised tool refused by actual Claude MCP boundary",e)};if schema["type"]!="object"{t.Fatal("not an MCP object")}})}
}
