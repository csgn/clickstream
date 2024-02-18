package event

import (
	"encoding/json"
	"fmt"

	"github.com/go-playground/validator/v10"
)

type channel string

const (
	web       channel = "web"
	mobile    channel = "mobile"
	mobileWeb channel = "mobile-web"
)

type variant string

const (
	pageview variant = "pageview"
)

type Event struct {
	Channel   channel `json:"channel" validate:"required"`
	Variant   variant `json:"variant" validate:"required"`
	Pid       string  `json:"pid" validate:"required,min=5"`
	CreatedAt int64   `json:"createdAt" validate:"required"`
	ViewedUrl string  `json:"viewedUrl" validate:"required,http_url"`
}

func (e *Event) Validate() error {
	v := validator.New()
	return v.Struct(e)
}

func (c channel) Validate() error {
	switch c {
	case web:
	case mobile:
	case mobileWeb:
		break
	default:
		return fmt.Errorf("Invalid channel type")
	}

	return nil
}

func (c variant) Validate() error {
	switch c {
	case pageview:
		break
	default:
		return fmt.Errorf("Invalid variant type")
	}

	return nil
}

func Unmarshal(b []byte) (*Event, error) {
	event := Event{}
	err := json.Unmarshal(b, &event)
	if err != nil {
		return nil, err
	}

	err = event.Validate()
	if err != nil {
		return nil, err
	}

	err = event.Channel.Validate()
	if err != nil {
		return nil, err
	}

	err = event.Variant.Validate()
	if err != nil {
		return nil, err
	}

	return &event, nil
}

func Marshal(e *Event) ([]byte, error) {
	return json.Marshal(e)
}
