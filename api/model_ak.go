package api

type Ak struct {
	AccesskeyId     string `json:"accesskey_id"`
	AccesskeySecret string `json:"accesskey_secret"`
	SecurityToken   string `json:"security_token"`
}
