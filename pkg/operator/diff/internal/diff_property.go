package internal

import (
	"encoding/json"

	"github.com/operator-framework/operator-registry/alpha/property"
)

const TypeChannel = "olm.channel"

type DiffChannelProperty struct {
	ChannelName string `json:"channelName"`
	Priority    int    `json:"priority"`
}

type DiffProperties struct {
	property.Properties
	Channels []DiffChannelProperty
}

func Parse(in []property.Property) (*DiffProperties, error) {
	var out DiffProperties
	for i, prop := range in {
		switch prop.Type {
		case property.TypePackage:
			var p property.Package
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, property.ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.Packages = append(out.Packages, p)
		case property.TypePackageRequired:
			var p property.PackageRequired
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, property.ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.PackagesRequired = append(out.PackagesRequired, p)
		case property.TypeGVK:
			var p property.GVK
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, property.ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.GVKs = append(out.GVKs, p)
		case property.TypeGVKRequired:
			var p property.GVKRequired
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, property.ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.GVKsRequired = append(out.GVKsRequired, p)
		case property.TypeBundleObject:
			var p property.BundleObject
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, property.ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.BundleObjects = append(out.BundleObjects, p)
		case TypeChannel:
			var p DiffChannelProperty
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, property.ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.Channels = append(out.Channels, p)
		default:
			var p json.RawMessage
			if err := json.Unmarshal(prop.Value, &p); err != nil {
				return nil, property.ParseError{Idx: i, Typ: prop.Type, Err: err}
			}
			out.Others = append(out.Others, prop)
		}
	}
	return &out, nil
}

func MustBuildChannelPriority(name string, priority int) property.Property {
	return property.MustBuild(&DiffChannelProperty{ChannelName: name, Priority: priority})
}
