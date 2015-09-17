package orient

// List of supported server-side script languages
const (
	LangSQL    = ScriptLang("sql")
	LangJS     = ScriptLang("javascript")
	LangGroovy = ScriptLang("groovy")
)

// ScriptLang is a type for supported server-side script languages
type ScriptLang string

// Function is a server-side function description
type Function struct {
	Name   string
	Lang   ScriptLang
	Params []string
	Idemp  bool // is idempotent
	Code   string
}
