package main

import (
	"encoding/json"
	"log"

	"github.com/OKI-Mesh/CoreScope/api"
)

// The operator writes config.json and theme.json by hand, so their blocks reach
// the server as untyped JSON objects. The functions below copy those objects
// into the generated response types. A value of the wrong type is logged and
// left out, and a key the web client does not know is dropped.

// decodeOperatorBlock copies the operator's JSON object into a T. It returns nil
// when the operator set nothing, which the API reports as null.
func decodeOperatorBlock[T any](m map[string]interface{}, what string) *T {
	if m == nil {
		return nil
	}
	var out T
	raw, err := json.Marshal(m)
	if err != nil {
		log.Printf("[config] %s: cannot read operator settings: %v", what, err)
		return &out
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		log.Printf("[config] %s: ignored some operator settings: %v", what, err)
	}
	return &out
}

// overlayOperatorBlocks lays each operator object over dst in order. Keys the
// object leaves out keep the value dst already has.
func overlayOperatorBlocks[T any](dst *T, what string, overlays ...map[string]interface{}) {
	for _, o := range overlays {
		if len(o) == 0 {
			continue
		}
		raw, err := json.Marshal(o)
		if err != nil {
			log.Printf("[config] %s: cannot read operator settings: %v", what, err)
			continue
		}
		if err := json.Unmarshal(raw, dst); err != nil {
			log.Printf("[config] %s: ignored some operator settings: %v", what, err)
		}
	}
}

// overlayStrings returns base with every string value from the overlays laid
// over it. Values that are not strings are left out.
func overlayStrings(base map[string]string, overlays ...map[string]interface{}) map[string]string {
	out := make(map[string]string, len(base))
	for k, v := range base {
		out[k] = v
	}
	for _, o := range overlays {
		for k, v := range o {
			if s, ok := v.(string); ok {
				out[k] = s
			}
		}
	}
	return out
}

// homeOverlay is the operator's home-page block. A nil field means the
// operator did not set it.
type homeOverlay struct {
	HeroTitle    *string         `json:"heroTitle"`
	HeroSubtitle *string         `json:"heroSubtitle"`
	Steps        *[]api.HomeStep `json:"steps"`
	FooterLinks  *[]api.HomeLink `json:"footerLinks"`
	Checklist    *[]api.HomeStep `json:"checklist"`
}

func (o homeOverlay) applyTo(h *api.HomeConfig) {
	if o.HeroTitle != nil {
		h.HeroTitle = *o.HeroTitle
	}
	if o.HeroSubtitle != nil {
		h.HeroSubtitle = *o.HeroSubtitle
	}
	if o.Steps != nil {
		h.Steps = *o.Steps
	}
	if o.FooterLinks != nil {
		h.FooterLinks = *o.FooterLinks
	}
	if o.Checklist != nil {
		h.Checklist = o.Checklist
	}
}

func defaultThemeBranding() api.ThemeBranding {
	return api.ThemeBranding{
		SiteName: "CoreScope",
		Tagline:  "Real-time MeshCore LoRa mesh network analyzer",
	}
}

func defaultLightThemeColors() api.ThemeColors {
	return api.ThemeColors{
		Accent:       "#4a9eff",
		AccentHover:  "#6db3ff",
		NavBg:        "#0f0f23",
		NavBg2:       "#1a1a2e",
		NavText:      "#ffffff",
		NavTextMuted: "#cbd5e1",
		Background:   "#f4f5f7",
		Text:         "#1a1a2e",
		TextMuted:    "#5b6370",
		Border:       "#e2e5ea",
		Surface1:     "#ffffff",
		Surface2:     "#ffffff",
		Surface3:     "#ffffff",
		SectionBg:    "#eef2ff",
		CardBg:       "#ffffff",
		ContentBg:    "#f4f5f7",
		DetailBg:     "#ffffff",
		InputBg:      "#ffffff",
		RowStripe:    "#f9fafb",
		RowHover:     "#eef2ff",
		SelectedBg:   "#dbeafe",
		StatusGreen:  "#22c55e",
		StatusYellow: "#eab308",
		StatusRed:    "#ef4444",
	}
}

func defaultDarkThemeColors() api.ThemeColors {
	return api.ThemeColors{
		Accent:       "#4a9eff",
		AccentHover:  "#6db3ff",
		NavBg:        "#0f0f23",
		NavBg2:       "#1a1a2e",
		NavText:      "#ffffff",
		NavTextMuted: "#cbd5e1",
		Background:   "#0f0f23",
		Text:         "#e2e8f0",
		TextMuted:    "#a8b8cc",
		Border:       "#334155",
		Surface1:     "#1a1a2e",
		Surface2:     "#232340",
		Surface3:     "#2d2d50",
		SectionBg:    "#1e1e34",
		CardBg:       "#1a1a2e",
		ContentBg:    "#0f0f23",
		DetailBg:     "#232340",
		InputBg:      "#1e1e34",
		RowStripe:    "#1e1e34",
		RowHover:     "#2d2d50",
		SelectedBg:   "#1e3a5f",
		StatusGreen:  "#22c55e",
		StatusYellow: "#eab308",
		StatusRed:    "#ef4444",
	}
}

func defaultNodeColors() map[string]string {
	return map[string]string{
		"repeater":  "#dc2626",
		"companion": "#2563eb",
		"room":      "#16a34a",
		"sensor":    "#d97706",
		"observer":  "#8b5cf6",
	}
}

func defaultTypeColors() map[string]string {
	return map[string]string{
		"ADVERT":   "#22c55e",
		"GRP_TXT":  "#3b82f6",
		"TXT_MSG":  "#f59e0b",
		"ACK":      "#6b7280",
		"REQ":      "#a855f7",
		"RESPONSE": "#06b6d4",
		"TRACE":    "#ec4899",
		"PATH":     "#14b8a6",
		"ANON_REQ": "#f43f5e",
		"UNKNOWN":  "#6b7280",
	}
}

func defaultHomeConfig() api.HomeConfig {
	return api.HomeConfig{
		HeroTitle:    "CoreScope",
		HeroSubtitle: "Real-time MeshCore LoRa mesh network analyzer",
		// #1648 M5: defaults use 'ph:<name>' Phosphor sprite tokens.
		// The frontend render path (home.js _renderHomeGlyph, customize-v2.js
		// renderConfigGlyph) ALSO accepts legacy emoji strings, so any
		// operator config.json that still stores raw emoji values continues
		// to render as-is — this changes ONLY the built-in default set.
		Steps: []api.HomeStep{
			{Emoji: "ph:bluetooth", Title: "Connect via Bluetooth", Description: "Flash **BLE companion** firmware from [MeshCore Flasher](https://flasher.meshcore.io/).\n- Screenless devices: default PIN `123456`\n- Screen devices: random PIN shown on display\n- If pairing fails: forget device, reboot, re-pair"},
			{Emoji: "ph:radio", Title: "Set the right frequency preset", Description: "**US Recommended:**\n`910.525 MHz · BW 62.5 kHz · SF 7 · CR 5`\nSelect **\"US Recommended\"** in the app or flasher."},
			{Emoji: "ph:broadcast", Title: "Advertise yourself", Description: "Tap the signal icon → **Flood** to broadcast your node to the mesh. Companions only advert when you trigger it manually."},
			{Emoji: "ph:repeat", Title: "Check \"Heard N repeats\"", Description: "- **\"Sent\"** = transmitted, no confirmation\n- **\"Heard 0 repeats\"** = no repeater picked it up\n- **\"Heard 1+ repeats\"** = you're on the mesh!"},
		},
		FooterLinks: []api.HomeLink{
			{Label: "ph:package Packets", Url: "#/packets"},
			{Label: "ph:map-trifold Network Map", Url: "#/map"},
		},
	}
}

// defaultMarkerStroke mirrors the :root values in public/style.css, so a
// visitor with no config and no override gets the same outline as the static
// CSS fallback (#1488).
func defaultMarkerStroke() api.MarkerStroke {
	return api.MarkerStroke{Color: "rgba(255,255,255,0.85)", Width: 1, Opacity: 1}
}

// clientMapConfig returns the operator's map block, without the provider
// entries that are not objects. config.example.json keeps its _comment_ notes
// there, as strings.
func clientMapConfig(m map[string]interface{}) *api.ClientMap {
	if m == nil {
		return nil
	}
	tiles, hasTiles := m["tiles"].(map[string]interface{})
	if !hasTiles {
		return decodeOperatorBlock[api.ClientMap](m, "map")
	}
	cleaned := make(map[string]interface{}, len(tiles))
	for k, v := range tiles {
		cleaned[k] = v
	}
	if providers, ok := tiles["providers"].(map[string]interface{}); ok {
		objects := make(map[string]interface{}, len(providers))
		for name, p := range providers {
			if _, isObject := p.(map[string]interface{}); isObject {
				objects[name] = p
			}
		}
		cleaned["providers"] = objects
	}
	return decodeOperatorBlock[api.ClientMap](map[string]interface{}{"tiles": cleaned}, "map")
}

// numberMap returns the numeric entries of m. Values that are not numbers are
// left out.
func numberMap(m map[string]interface{}) api.CacheTTLConfig {
	out := make(api.CacheTTLConfig, len(m))
	for k, v := range m {
		switch n := v.(type) {
		case float64:
			out[k] = n
		case float32:
			out[k] = float64(n)
		case int:
			out[k] = float64(n)
		case int64:
			out[k] = float64(n)
		case json.Number:
			if f, err := n.Float64(); err == nil {
				out[k] = f
			}
		}
	}
	return out
}

// externalUrls returns the operator's externalUrls block, or nil when none is
// configured. Entries that are not strings are dropped.
func externalUrls(m map[string]interface{}) *api.ExternalUrls {
	if m == nil {
		return nil
	}
	out := make(api.ExternalUrls, len(m))
	for k, v := range m {
		if str, ok := v.(string); ok {
			out[k] = str
		}
	}
	return &out
}

// latLonList converts a polygon to its wire form.
func latLonList(points [][2]float64) api.LatLonList {
	out := make(api.LatLonList, len(points))
	for i, p := range points {
		out[i] = api.LatLon{p[0], p[1]}
	}
	return out
}
