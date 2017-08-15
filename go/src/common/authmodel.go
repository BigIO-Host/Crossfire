package common

import (
	"time"
)

type ProjectKeyModel struct {
	ProjectID string `datastore:"ProjectId"`
	Locked    string `datastore:"Locked"`
	KeyString string `datastore:"KeyString"`
}

type ProjectStatusModel struct {
	ProjectID   string    `datastore:"ProjectId" json:"ProjectId"`
	ProjectDesc string    `datastore:"ProjectDesc" json:"ProjectDesc"`
	AuthPulse   string    `datastore:"AuthPulse" json:"AuthPulse"`
	AuthPredict string    `datastore:"AuthPredict" json:"AuthPredict"`
	AuthAction  string    `datastore:"AuthAction" json:"AuthAction"`
	Deleted     string    `datastore:"Deleted" json:"Deleted"`
	CreateDate  time.Time `datastore:"CreateDate" json:"CreateDate"`
	SsnConf     string    `datastore:"SsnConf" json:"SsnConf"`
}
