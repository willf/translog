package worker_test

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/willf/translog/worker"
)

func TestConfiguredElasticSearchHostDefault(t *testing.T) {
	if worker.ConfiguredElasticSearchHost() != "localhost" {
		t.Errorf("expected default ES host to be localhost, but was %v", worker.ConfiguredElasticSearchHost())
	}
}

func TestConfiguredElasticSearchPortDefault(t *testing.T) {
	if worker.ConfiguredElasticSearchPort() != 9200 {
		t.Errorf("expected default ES host to be 9200, but was %v", worker.ConfiguredElasticSearchPort())
	}
}

func TestConfiguredElasticSearchHostDefined(t *testing.T) {
	host := "es-production.example.com"
	viper.Set("es.host", host)
	if worker.ConfiguredElasticSearchHost() != host {
		t.Errorf("expected ES host to be %v, but was %v", host, worker.ConfiguredElasticSearchHost())
	}
}

func TestConfiguredElasticSearchPortDefined(t *testing.T) {
	port := 9400
	viper.Set("es.port", port)
	if worker.ConfiguredElasticSearchPort() != port {
		t.Errorf("expected ES port to be %v, but was %v", port, worker.ConfiguredElasticSearchPort())
	}
}

func TestConfiguredElasticSearchSchemeDefault(t *testing.T) {
	if worker.ConfiguredElasticSearchScheme() != "http" {
		t.Errorf("expected default ES scheme to be http, but was %v", worker.ConfiguredElasticSearchScheme())
	}
}

func TestConfiguredElasticSearchSchemeDefined(t *testing.T) {
	scheme := "https"
	viper.Set("es.scheme", scheme)
	if worker.ConfiguredElasticSearchScheme() != scheme {
		t.Errorf("expected ES scheme to be %v, but was %v", scheme, worker.ConfiguredElasticSearchScheme())
	}
}
