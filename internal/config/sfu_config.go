package config

import (
	"time"

	"telesrv/internal/branding"
)

type SFUConfigYAML struct {
	Version    int `yaml:"version"`
	CommonYAML `yaml:",inline"`
	SFU        SFUYAML       `yaml:"sfu"`
	GroupCall  GroupCallYAML `yaml:"group_call"`
}

type SFUYAML struct {
	UDPPort                   *int           `yaml:"udp_port"`
	AdvertiseIP               string         `yaml:"advertise_ip"`
	InstanceTTL               string         `yaml:"instance_ttl"`
	InstanceHeartbeatInterval string         `yaml:"instance_heartbeat_interval"`
	MaxActiveCalls            *int           `yaml:"max_active_calls"`
	Control                   GRPCServerYAML `yaml:"control"`
}

// LoadSFU reads the SFU role YAML config and returns the runtime snapshot used by the SFU process.
func LoadSFU() (SFUConfig, error) {
	cfg, err := loadRoleYAML(roleSFU, func(b *envBuilder, y SFUConfigYAML) error {
		if err := applyCommonYAML(b, y.CommonYAML); err != nil {
			return err
		}
		b.setInt("TELESRV_SFU_UDP_PORT", y.SFU.UDPPort)
		b.setString("TELESRV_SFU_ADVERTISE_IP", y.SFU.AdvertiseIP)
		if err := b.setDuration("TELESRV_SFU_INSTANCE_TTL", y.SFU.InstanceTTL); err != nil {
			return err
		}
		if err := b.setDuration("TELESRV_SFU_INSTANCE_HEARTBEAT_INTERVAL", y.SFU.InstanceHeartbeatInterval); err != nil {
			return err
		}
		b.setInt("TELESRV_SFU_INSTANCE_MAX_ACTIVE_CALLS", y.SFU.MaxActiveCalls)
		applySFUControlServerYAML(b, y.SFU.Control)
		b.setString("TELESRV_GROUPCALL_CONTROL_URL", y.GroupCall.ControlURL)
		b.setString("TELESRV_GROUPCALL_CONTROL_TOKEN", y.GroupCall.ControlToken)
		return nil
	})
	if err != nil {
		return SFUConfig{}, err
	}
	return sfuConfigFromConfig(cfg), nil
}

type SFUConfig struct {
	DebugAddr string

	Branding           branding.Config
	PremiumBotUsername string
	PremiumBotUserID   int64

	AdvertiseIP                   string
	RedisAddr                     string
	RedisPassword                 string
	RedisDB                       int
	InstanceID                    string
	GroupCallControlURL           string
	GroupCallControlToken         string
	SFUUDPPort                    int
	SFUAdvertiseIP                string
	SFUInstanceTTL                time.Duration
	SFUInstanceHeartbeatInterval  time.Duration
	SFUInstanceMaxActiveCalls     int
	SFUControlGRPCAddr            string
	SFUControlGRPCURL             string
	SFUControlGRPCTLSCertFile     string
	SFUControlGRPCTLSKeyFile      string
	SFUControlGRPCTLSClientCAFile string
	SFUControlToken               string
}

func (c SFUConfig) ProcessGlobals() ProcessGlobals {
	return ProcessGlobals{Branding: c.Branding, PremiumBotUsername: c.PremiumBotUsername, PremiumBotUserID: c.PremiumBotUserID}
}

func (c SFUConfig) DebugAddress() string {
	return c.DebugAddr
}

func sfuConfigFromConfig(c Config) SFUConfig {
	return SFUConfig{
		DebugAddr: c.DebugAddr, Branding: c.Branding, PremiumBotUsername: c.PremiumBotUsername, PremiumBotUserID: c.PremiumBotUserID,
		AdvertiseIP: c.AdvertiseIP, RedisAddr: c.RedisAddr, RedisPassword: c.RedisPassword, RedisDB: c.RedisDB, InstanceID: c.InstanceID,
		GroupCallControlURL: c.GroupCallControlURL, GroupCallControlToken: c.GroupCallControlToken,
		SFUUDPPort: c.SFUUDPPort, SFUAdvertiseIP: c.SFUAdvertiseIP, SFUInstanceTTL: c.SFUInstanceTTL,
		SFUInstanceHeartbeatInterval: c.SFUInstanceHeartbeatInterval, SFUInstanceMaxActiveCalls: c.SFUInstanceMaxActiveCalls,
		SFUControlGRPCAddr: c.SFUControlGRPCAddr, SFUControlGRPCURL: c.SFUControlGRPCURL,
		SFUControlGRPCTLSCertFile: c.SFUControlGRPCTLSCertFile, SFUControlGRPCTLSKeyFile: c.SFUControlGRPCTLSKeyFile,
		SFUControlGRPCTLSClientCAFile: c.SFUControlGRPCTLSClientCAFile, SFUControlToken: c.SFUControlToken,
	}
}
