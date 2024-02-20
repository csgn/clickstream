package main

import (
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
	CreatedAt string  `json:"createdAt" validate:"required"`
	ViewedUrl string  `json:"viewedUrl" validate:"required,uri"`
}

func (c channel) validate() error {
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

func (c variant) validate() error {
	switch c {
	case pageview:
		break
	default:
		return fmt.Errorf("Invalid variant type")
	}

	return nil
}

func (e *Event) Validate() error {
	v := validator.New()

	err := v.Struct(e)
	if err != nil {
		return err
	}

	err = e.Channel.validate()
	if err != nil {
		return err
	}

	err = e.Variant.validate()
	if err != nil {
		return err
	}

	return nil
}
