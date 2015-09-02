package orient

const (
	LangSQL    = ScriptLang("sql")
	LangJS     = ScriptLang("javascript")
	LangGroovy = ScriptLang("groovy")
)

type ScriptLang string

type Function struct {
	Name   string
	Lang   ScriptLang
	Params []string
	Idemp  bool // is idempotent
	Code   string
}
