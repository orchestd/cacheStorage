package models

type Chain struct {
	Id              string
	Name            string
	DefaultLanguage string
	Settings        []ChainSetting
}

type ChainSetting struct {
	OptionId    string
	OptionsType string
	OptionValue string
}
