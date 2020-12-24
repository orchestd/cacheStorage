package models

type Store struct {
	Id           string
	Name         string
	DueDateStart string
	DueDateEnd   string
	CurrencyId   string
	Language     string
	CompanyTaxId string
	ChainId      string
	Settings     []StoreSetting
}

type StoreSetting struct {
	Setting string
	Value   string
}
