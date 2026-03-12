package conf

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	WorkerType     string `yaml:"workerType"`
	WorkerID       int    `yaml:"workerId"`
	WorkerRegion   string `yaml:"workerRegion"`
	WorkerMgrAddr  string `yaml:"workerMgrAddr"`
	AuthAPIAddr    string `yaml:"authApiAddr"`
	AppSecret      string `yaml:"appSecret"`
	PublishURL     string `yaml:"publishUrl"`
	PublishURL2    string `yaml:"publishUrl2"`
	InputFile      string `yaml:"inputFile"`
	VideoLayout    string `yaml:"videoLayout"`
	PublishOnReady bool   `yaml:"publishOnReady"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse yaml %q: %w", path, err)
	}

	overrideByEnv(cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if strings.TrimSpace(c.WorkerType) == "" {
		return fmt.Errorf("workerType is required")
	}
	if c.WorkerID <= 0 {
		return fmt.Errorf("workerId must be > 0")
	}
	if strings.TrimSpace(c.WorkerRegion) == "" {
		return fmt.Errorf("workerRegion is required")
	}
	if strings.TrimSpace(c.WorkerMgrAddr) == "" {
		return fmt.Errorf("workerMgrAddr is required")
	}
	if strings.TrimSpace(c.PublishURL) == "" && strings.TrimSpace(c.PublishURL2) == "" {
		return fmt.Errorf("publishUrl and publishUrl2 cannot both be empty")
	}
	if strings.TrimSpace(c.InputFile) == "" {
		return fmt.Errorf("inputFile is required")
	}
	c.VideoLayout = strings.ToLower(strings.TrimSpace(c.VideoLayout))
	if c.VideoLayout == "" {
		c.VideoLayout = "portrait"
	}
	switch c.VideoLayout {
	case "portrait", "landscape", "both":
	default:
		return fmt.Errorf("videoLayout must be one of: portrait, landscape, both")
	}
	return nil
}

func overrideByEnv(cfg *Config) {
	if v := strings.TrimSpace(os.Getenv("VPUBLISHER_WORKER_TYPE")); v != "" {
		cfg.WorkerType = v
	}
	if v := strings.TrimSpace(os.Getenv("VPUBLISHER_WORKER_ID")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			cfg.WorkerID = id
		}
	}
	if v := strings.TrimSpace(os.Getenv("VPUBLISHER_WORKER_REGION")); v != "" {
		cfg.WorkerRegion = v
	}
	if v := strings.TrimSpace(os.Getenv("VPUBLISHER_WORKER_MGR_ADDR")); v != "" {
		cfg.WorkerMgrAddr = v
	}
	if v := strings.TrimSpace(os.Getenv("VPUBLISHER_AUTH_API_ADDR")); v != "" {
		cfg.AuthAPIAddr = v
	}
	if v := strings.TrimSpace(os.Getenv("VPUBLISHER_APP_SECRET")); v != "" {
		cfg.AppSecret = v
	}
	if v := strings.TrimSpace(os.Getenv("VPUBLISHER_PUBLISH_URL")); v != "" {
		cfg.PublishURL = v
	}
	if v := strings.TrimSpace(os.Getenv("VPUBLISHER_PUBLISH_URL2")); v != "" {
		cfg.PublishURL2 = v
	}
	if v := strings.TrimSpace(os.Getenv("VPUBLISHER_INPUT_FILE")); v != "" {
		cfg.InputFile = v
	}
	if v := strings.TrimSpace(os.Getenv("VPUBLISHER_VIDEO_LAYOUT")); v != "" {
		cfg.VideoLayout = v
	}
	if v := strings.TrimSpace(os.Getenv("VPUBLISHER_PUBLISH_ON_READY")); v != "" {
		if enabled, err := strconv.ParseBool(v); err == nil {
			cfg.PublishOnReady = enabled
		}
	}
}
