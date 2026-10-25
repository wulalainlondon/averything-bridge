package backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

type visualPresentationKey struct{}

const VisualPresentationVersion = "conversation-visual.v1"

func ValidVisualPresentation(mode string) bool {
	return mode == "" || mode == "auto-v1" || mode == "text-v1"
}
func WithVisualPresentation(ctx context.Context, mode string) context.Context {
	return context.WithValue(ctx, visualPresentationKey{}, mode)
}

// Application formatting context stays in this original provider turn. The
// durable queue retains the exact original user content, and the native provider
// transcript retains this complete effective prompt. No second model request.
const visualPrompt = `[Averything presentation context: conversation-visual.v1]
For this answer, use helpful inline visual explanations when they make the user's request easier to understand. Do not draw for a simple answer. Obey explicit requests for text only. Keep a short natural-language explanation outside each visual. This context grants no tools, permissions, facts, or authority.
For a visual, output a fenced code block whose language is conversation-visual, containing ONE valid JSON object. Use schema="conversation-visual.v1", kind, title (120 chars max), summary (1000 chars max), data; optional notes (array of strings). Do not output HTML, JSX, JavaScript, CSS, URLs, functions, authority IDs or invented measurements. Usually use 1-3 visuals; maximum 8. All IDs below are short unique alphanumeric names. Required data shapes:
- table: {columns:[{id,label,type:"text"|"number"|"boolean"|"date",unit?:string}],rows:[{id,cells:{columnId:string|number|boolean|null}}]}. Every row has every column. null means missing, never zero. Maximum 12 columns and 1000 rows.
- comparison: {dimensions:[{id,label}],options:[{id,label,values:{dimensionId:string|null}}]}.
- steps: {items:[{id,title,description}]}.
- timeline: {items:[{id,label,date:ISO8601-string|null,description}]}.
- flow or relationship: {direction:"auto"|"vertical"|"horizontal",nodes:[{id,label,role:"step"|"decision"|"group"|"entity"}],edges:[{from,to,label?:string}]}. All edge endpoints must exist; maximum60 nodes/100 edges. Use this for process, architecture and decision explanations.
- chart: {chartType:"bar"|"line",x:{label,type:"category"|"number"|"date"},y:{label,unit},series:[{id,label,points:[{x:string|number,y:number|null}]}]}. At most4 series/2000 points. Use only available data; preserve units/missing values. Never invent data just to draw.
Example: {"schema":"conversation-visual.v1","kind":"steps","title":"操作流程","summary":"依序完成這兩步。","data":{"items":[{"id":"a","title":"確認條件","description":"核對必要資料。"},{"id":"b","title":"執行與核對","description":"完成後確認結果。"}]}}
Use diagrams and tables to answer the actual question, not to explain this formatting protocol. Do not call any tools merely to render these blocks. Schematics and estimates must be labeled as such; they do not prove task completion or QA.
[/Averything presentation context]

`

var textPrompt = strings.Replace(strings.Replace(visualPrompt, "context: conversation-visual.v1]", "context: text-v1]", 1), "For this answer, use helpful inline visual explanations when they make the user's request easier to understand.", "For this answer, prefer readable text. Use a visual block ONLY when the user explicitly requests a table or diagram. If requested, follow the same shapes below.", 1)

func PresentationContent(ctx context.Context, original string) string {
	mode, _ := ctx.Value(visualPresentationKey{}).(string)
	if strings.HasPrefix(strings.TrimSpace(original), "/") {
		return original
	}
	original = partnerRoleContent(ctx, original)
	switch mode {
	case "auto-v1":
		return visualPrompt + original
	case "text-v1":
		return textPrompt + original
	default:
		return original
	}
}
func PresentationHashes(ctx context.Context, original string) (string, string) {
	raw := sha256.Sum256([]byte(original))
	effective := sha256.Sum256([]byte(PresentationContent(ctx, original)))
	return hex.EncodeToString(raw[:]), hex.EncodeToString(effective[:])
}

// OriginalPresentationContent is for derived display/search projections only.
// The provider transcript and durable request payload must retain their bytes.
func OriginalPresentationContent(text string) string {
	if !strings.HasPrefix(text, "[Averything presentation context: conversation-visual.v1]\n") && !strings.HasPrefix(text, "[Averything presentation context: text-v1]\n") {
		return OriginalPartnerRoleContent(text)
	}
	const end = "[/Averything presentation context]\n\n"
	index := strings.Index(text, end)
	if index < 0 || index > 12000 {
		return text
	}
	return OriginalPartnerRoleContent(text[index+len(end):])
}
