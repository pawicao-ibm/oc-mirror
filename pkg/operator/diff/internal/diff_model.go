package internal

import (
	"fmt"
	"sort"
	"strings"

	"github.com/blang/semver/v4"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/operator-framework/operator-registry/alpha/model"
	"github.com/operator-framework/operator-registry/alpha/property"
	"k8s.io/apimachinery/pkg/util/sets"
)

type DiffModel map[string]*DiffPackage

type DiffPackage struct {
	model.Package
	DefaultChannel *DiffChannel
	Channels       map[string]*DiffChannel
}

type DiffChannel struct {
	model.Channel
	Package    *DiffPackage
	Bundles    map[string]*DiffBundle
	Properties []property.Property
}

type DiffBundle struct {
	model.Bundle
	Package *DiffPackage
	Channel *DiffChannel
}

func (c DiffChannel) Head() (*DiffBundle, error) {
	incoming := map[string]int{}
	for _, b := range c.Bundles {
		if b.Replaces != "" {
			incoming[b.Replaces]++
		}
		for _, skip := range b.Skips {
			incoming[skip]++
		}
	}
	var heads []*DiffBundle
	for _, b := range c.Bundles {
		if _, ok := incoming[b.Name]; !ok {
			heads = append(heads, b)
		}
	}
	if len(heads) == 0 {
		return nil, fmt.Errorf("no channel head found in graph")
	}
	if len(heads) > 1 {
		var headNames []string
		for _, head := range heads {
			headNames = append(headNames, head.Name)
		}
		sort.Strings(headNames)
		return nil, fmt.Errorf("multiple channel heads found in graph: %s", strings.Join(headNames, ", "))
	}
	return heads[0], nil
}

func (m DiffModel) AddBundle(b DiffBundle) {
	if _, present := m[b.Package.Name]; !present {
		m[b.Package.Name] = b.Package
	}
	p := m[b.Package.Name]
	b.Package = p

	if ch, ok := p.Channels[b.Channel.Name]; ok {
		b.Channel = ch
		ch.Bundles[b.Name] = &b
	} else {
		newCh := &DiffChannel{
			Channel: model.Channel{
				Name: b.Channel.Name,
			},
			Package: p,
			Bundles: make(map[string]*DiffBundle),
		}
		b.Channel = newCh
		newCh.Bundles[b.Name] = &b
		p.Channels[newCh.Name] = newCh
	}

	if p.DefaultChannel == nil {
		p.DefaultChannel = b.Channel
	}
}

func (m DiffModel) Normalize() {
	for _, pkg := range m {
		for _, ch := range pkg.Channels {
			for _, b := range ch.Bundles {
				for i := range b.Properties {
					// Ensure property value is encoded in a standard way.
					if normalized, err := property.Build(&b.Properties[i]); err == nil {
						b.Properties[i] = *normalized
					}
				}
			}
		}
	}
}

// TODO: The current form of the Functions below is just to check if  should be refactored not to duplicate

func ConvertDeclcfgToDiffModel(inputDeclcfg declcfg.DeclarativeConfig) (DiffModel, error) {
	mpkgs := DiffModel{}
	defaultChannels := map[string]string{}
	for _, p := range inputDeclcfg.Packages {
		if p.Name == "" {
			return nil, fmt.Errorf("config contains package with no name")
		}

		if _, ok := mpkgs[p.Name]; ok {
			return nil, fmt.Errorf("duplicate package %q", p.Name)
		}

		mpkg := &DiffPackage{
			Package: model.Package{
				Name:        p.Name,
				Description: p.Description,
			},
			Channels: map[string]*DiffChannel{},
		}
		if p.Icon != nil {
			mpkg.Icon = &model.Icon{
				Data:      p.Icon.Data,
				MediaType: p.Icon.MediaType,
			}
		}
		defaultChannels[p.Name] = p.DefaultChannel
		mpkgs[p.Name] = mpkg
	}

	channelDefinedEntries := map[string]sets.String{}
	for _, c := range inputDeclcfg.Channels {
		mpkg, ok := mpkgs[c.Package]
		if !ok {
			return nil, fmt.Errorf("unknown package %q for channel %q", c.Package, c.Name)
		}

		if c.Name == "" {
			return nil, fmt.Errorf("package %q contains channel with no name", c.Package)
		}

		if _, ok := mpkg.Channels[c.Name]; ok {
			return nil, fmt.Errorf("package %q has duplicate channel %q", c.Package, c.Name)
		}

		mch := &DiffChannel{
			Channel: model.Channel{
				Name: c.Name,
			},
			Package:    mpkg,
			Bundles:    map[string]*DiffBundle{},
			Properties: c.Properties,
		}

		cde := sets.NewString()
		for _, entry := range c.Entries {
			if _, ok := mch.Bundles[entry.Name]; ok {
				return nil, fmt.Errorf("invalid package %q, channel %q: duplicate entry %q", c.Package, c.Name, entry.Name)
			}
			cde = cde.Insert(entry.Name)
			mch.Bundles[entry.Name] = &DiffBundle{
				Bundle: model.Bundle{
					Name:      entry.Name,
					Replaces:  entry.Replaces,
					Skips:     entry.Skips,
					SkipRange: entry.SkipRange,
				},
				Package: mpkg,
				Channel: mch,
			}
		}
		channelDefinedEntries[c.Package] = cde

		mpkg.Channels[c.Name] = mch

		defaultChannelName := defaultChannels[c.Package]
		if defaultChannelName == c.Name {
			mpkg.DefaultChannel = mch
		}
	}

	// packageBundles tracks the set of bundle names for each package
	// and is used to detect duplicate bundles.
	packageBundles := map[string]sets.String{}

	for _, b := range inputDeclcfg.Bundles {
		if b.Package == "" {
			return nil, fmt.Errorf("package name must be set for bundle %q", b.Name)
		}
		mpkg, ok := mpkgs[b.Package]
		if !ok {
			return nil, fmt.Errorf("unknown package %q for bundle %q", b.Package, b.Name)
		}

		bundles, ok := packageBundles[b.Package]
		if !ok {
			bundles = sets.NewString()
		}
		if bundles.Has(b.Name) {
			return nil, fmt.Errorf("package %q has duplicate bundle %q", b.Package, b.Name)
		}
		bundles.Insert(b.Name)
		packageBundles[b.Package] = bundles

		props, err := property.Parse(b.Properties)
		if err != nil {
			return nil, fmt.Errorf("parse properties for bundle %q: %v", b.Name, err)
		}

		if len(props.Packages) != 1 {
			return nil, fmt.Errorf("package %q bundle %q must have exactly 1 %q property, found %d", b.Package, b.Name, property.TypePackage, len(props.Packages))
		}

		if b.Package != props.Packages[0].PackageName {
			return nil, fmt.Errorf("package %q does not match %q property %q", b.Package, property.TypePackage, props.Packages[0].PackageName)
		}

		// Parse version from the package property.
		rawVersion := props.Packages[0].Version
		ver, err := semver.Parse(rawVersion)
		if err != nil {
			return nil, fmt.Errorf("error parsing bundle %q version %q: %v", b.Name, rawVersion, err)
		}

		channelDefinedEntries[b.Package] = channelDefinedEntries[b.Package].Delete(b.Name)
		found := false
		for _, mch := range mpkg.Channels {
			if mb, ok := mch.Bundles[b.Name]; ok {
				found = true
				mb.Image = b.Image
				mb.Properties = b.Properties
				mb.RelatedImages = declcfg.RelatedImagesToModelRelatedImages(b.RelatedImages)
				mb.CsvJSON = b.CsvJSON
				mb.Objects = b.Objects
				mb.PropertiesP = props
				mb.Version = ver
			}
		}
		if !found {
			return nil, fmt.Errorf("package %q, bundle %q not found in any channel entries", b.Package, b.Name)
		}
	}

	for pkg, entries := range channelDefinedEntries {
		if entries.Len() > 0 {
			return nil, fmt.Errorf("no olm.bundle blobs found in package %q for olm.channel entries %s", pkg, entries.List())
		}
	}

	for _, mpkg := range mpkgs {
		defaultChannelName := defaultChannels[mpkg.Name]
		if defaultChannelName != "" && mpkg.DefaultChannel == nil {
			dch := &DiffChannel{
				Channel: model.Channel{
					Name: defaultChannelName,
				},
				Package: mpkg,
				Bundles: map[string]*DiffBundle{},
			}
			mpkg.DefaultChannel = dch
			mpkg.Channels[dch.Name] = dch
		}
	}

	//if err := mpkgs.Validate(); err != nil {
	//	return nil, err
	//}
	mpkgs.Normalize()
	return mpkgs, nil
}

func ConvertDiffModelToDeclcfg(inputDiffModel DiffModel) declcfg.DeclarativeConfig {
	cfg := declcfg.DeclarativeConfig{}
	for _, mpkg := range inputDiffModel {
		channels, bundles := traverseModelChannels(*mpkg)

		var i *declcfg.Icon
		if mpkg.Icon != nil {
			i = &declcfg.Icon{
				Data:      mpkg.Icon.Data,
				MediaType: mpkg.Icon.MediaType,
			}
		}
		defaultChannel := ""
		if mpkg.DefaultChannel != nil {
			defaultChannel = mpkg.DefaultChannel.Name
		}
		cfg.Packages = append(cfg.Packages, declcfg.Package{
			Schema:         declcfg.SchemaPackage,
			Name:           mpkg.Name,
			DefaultChannel: defaultChannel,
			Icon:           i,
			Description:    mpkg.Description,
		})
		cfg.Channels = append(cfg.Channels, channels...)
		cfg.Bundles = append(cfg.Bundles, bundles...)
	}

	sort.Slice(cfg.Packages, func(i, j int) bool {
		return cfg.Packages[i].Name < cfg.Packages[j].Name
	})
	sort.Slice(cfg.Channels, func(i, j int) bool {
		if cfg.Channels[i].Package != cfg.Channels[j].Package {
			return cfg.Channels[i].Package < cfg.Channels[j].Package
		}
		return cfg.Channels[i].Name < cfg.Channels[j].Name
	})
	sort.Slice(cfg.Bundles, func(i, j int) bool {
		if cfg.Bundles[i].Package != cfg.Bundles[j].Package {
			return cfg.Bundles[i].Package < cfg.Bundles[j].Package
		}
		return cfg.Bundles[i].Name < cfg.Bundles[j].Name
	})

	return cfg
}

func traverseModelChannels(mpkg DiffPackage) ([]declcfg.Channel, []declcfg.Bundle) {
	channels := []declcfg.Channel{}
	bundleMap := map[string]*declcfg.Bundle{}

	for _, ch := range mpkg.Channels {
		fmt.Printf("%+v\n", ch)
		// initialize channel
		c := declcfg.Channel{
			Schema:     declcfg.SchemaChannel,
			Name:       ch.Name,
			Entries:    []declcfg.ChannelEntry{},
			Properties: ch.Properties,
			Package:    ch.Package.Name,
		}

		for _, chb := range ch.Bundles {
			// populate channel entry
			c.Entries = append(c.Entries, declcfg.ChannelEntry{
				Name:      chb.Name,
				Replaces:  chb.Replaces,
				Skips:     chb.Skips,
				SkipRange: chb.SkipRange,
			})

			// create or update bundle
			b, ok := bundleMap[chb.Name]
			if !ok {
				b = &declcfg.Bundle{
					Schema:        declcfg.SchemaBundle,
					Name:          chb.Name,
					Package:       chb.Package.Name,
					Image:         chb.Image,
					RelatedImages: declcfg.ModelRelatedImagesToRelatedImages(chb.RelatedImages),
					CsvJSON:       chb.CsvJSON,
					Objects:       chb.Objects,
				}
				bundleMap[b.Name] = b
			}
			b.Properties = append(b.Properties, chb.Properties...)
		}

		// sort channel entries by name
		sort.Slice(c.Entries, func(i, j int) bool {
			return c.Entries[i].Name < c.Entries[j].Name
		})
		channels = append(channels, c)
	}

	var bundles []declcfg.Bundle
	for _, b := range bundleMap {
		b.Properties = property.Deduplicate(b.Properties)

		sort.Slice(b.Properties, func(i, j int) bool {
			if b.Properties[i].Type != b.Properties[j].Type {
				return b.Properties[i].Type < b.Properties[j].Type
			}
			return string(b.Properties[i].Value) < string(b.Properties[j].Value)
		})

		bundles = append(bundles, *b)
	}
	return channels, bundles
}
