package worker_test

import (
	"fmt"
	"testing"

	"github.com/spf13/viper"
	"github.com/willf/translog/worker"
)

func TestConfiguredElasticSearchHosstDefault(t *testing.T) {
	viper.Reset()
	if worker.ConfiguredElasticSearchHosts()[0] != "localhost" {
		t.Errorf("expected default ES host to be localhost, but was %v", worker.ConfiguredElasticSearchHosts())
	}
}

func TestConfiguredElasticSearchHostsDefined(t *testing.T) {
	viper.Reset()
	host := "es-production.example.com"
	hosts := make([]string, 1)
	hosts[0] = host
	viper.Set("es.hosts", hosts)
	fmt.Println(worker.ConfiguredElasticSearchHosts())
	if worker.ConfiguredElasticSearchHosts()[0] != host {
		t.Errorf("expected ES host to be %v, but was %v", host, worker.ConfiguredElasticSearchHosts())
	}
}

func TestConfiguredElasticSearchPortDefault(t *testing.T) {
	viper.Reset()
	if worker.ConfiguredElasticSearchPort() != 9200 {
		t.Errorf("expected default ES host to be 9200, but was %v", worker.ConfiguredElasticSearchPort())
	}
}

func TestConfiguredElasticSearchPortDefined(t *testing.T) {
	viper.Reset()
	port := 9400
	viper.Set("es.port", port)
	if worker.ConfiguredElasticSearchPort() != port {
		t.Errorf("expected ES port to be %v, but was %v", port, worker.ConfiguredElasticSearchPort())
	}
}

func TestConfiguredElasticSearchSchemeDefault(t *testing.T) {
	viper.Reset()
	if worker.ConfiguredElasticSearchScheme() != "http" {
		t.Errorf("expected default ES scheme to be http, but was %v", worker.ConfiguredElasticSearchScheme())
	}
}

func TestConfiguredElasticSearchSchemeDefined(t *testing.T) {
	viper.Reset()
	scheme := "https"
	viper.Set("es.scheme", scheme)
	if worker.ConfiguredElasticSearchScheme() != scheme {
		t.Errorf("expected ES scheme to be %v, but was %v", scheme, worker.ConfiguredElasticSearchScheme())
	}
}

func TestConfiguredElasticSearchMaxDefault(t *testing.T) {
	viper.Reset()
	if worker.ConfiguredElasticSearchMax() != 500 {
		t.Errorf("expected default ES max to be 500, but was %v", worker.ConfiguredElasticSearchMax())
	}
}

func TestConfiguredElasticSearchIndex(t *testing.T) {
	viper.Reset()
	index := "ia"
	viper.Set("es.index", index)
	if worker.ConfiguredElasticSearchIndex() != index {
		t.Errorf("expected ES index to be %v, but was %v", index, worker.ConfiguredElasticSearchIndex())
	}
}

func TestConfiguredElasticSearchIndexDefault(t *testing.T) {
	viper.Reset()
	if worker.ConfiguredElasticSearchIndex() != "analytics" {
		t.Errorf("expected default ES index to be analytics, but was %v", worker.ConfiguredElasticSearchIndex())
	}
}

func TestConfiguredElasticSearchDocumentType(t *testing.T) {
	viper.Reset()
	DocumentType := "thing"
	viper.Set("es.document_type", DocumentType)
	if worker.ConfiguredElasticSearchDocumentType() != DocumentType {
		t.Errorf("expected ES DocumentType to be %v, but was %v", DocumentType, worker.ConfiguredElasticSearchDocumentType())
	}
}

func TestConfiguredElasticSearchDocumentTypeDefault(t *testing.T) {
	viper.Reset()
	if worker.ConfiguredElasticSearchDocumentType() != "event" {
		t.Errorf("expected default ES DocumentType to be event, but was %v", worker.ConfiguredElasticSearchDocumentType())
	}
}

func TestConfiguredElasticSearchMock(t *testing.T) {
	viper.Reset()
	mocking := true
	viper.Set("es.mocking", mocking)
	if worker.ConfiguredElasticSearchMocking() != mocking {
		t.Errorf("expected ES mocking to be %v, but was %v", mocking, worker.ConfiguredElasticSearchMocking())
	}
}

func TestConfiguredElasticSearchDocumentMockingDefault(t *testing.T) {
	viper.Reset()
	if worker.ConfiguredElasticSearchMocking() != false {
		t.Errorf("expected default ES mocking to be false, but was %v", worker.ConfiguredElasticSearchMocking())
	}
}

func TestConfiguredElasticSearchUseDateSuffix(t *testing.T) {
	viper.Reset()
	use_suffix := true
	viper.Set("es.use_date_suffix", use_suffix)
	if worker.ConfiguredElasticSearchUseDateSuffix() != use_suffix {
		t.Errorf("expected ES use_date_suffix to be %v, but was %v", use_suffix, worker.ConfiguredElasticSearchUseDateSuffix())
	}
}

func TestConfiguredElasticSearchDocumentSuffixDefault(t *testing.T) {
	viper.Reset()
	if worker.ConfiguredElasticSearchUseDateSuffix() != false {
		t.Errorf("expected default ES use_date_suffix to be false, but was %v", worker.ConfiguredElasticSearchUseDateSuffix())
	}
}

func TestRoundRobin(t *testing.T) {
	viper.Reset()
	hosts := make([]string, 4)
	hosts[0] = "alpha"
	hosts[1] = "beta"
	hosts[2] = "gamma"
	hosts[3] = "delta"
	viper.Set("es.hosts", hosts)
	w := &worker.ElasticSearchWorker{}
	w.Init()
	host := w.NextHost()
	if host != "alpha" {
		t.Errorf("round robin on alpha 1 failed, got %v", host)
	}
	host = w.NextHost()
	if host != "beta" {
		t.Errorf("round robin on beta 1 failed, got %v", host)
	}
	host = w.NextHost()
	if host != "gamma" {
		t.Errorf("round robin on gamma 1 failed, got %v", host)
	}
	host = w.NextHost()
	if host != "delta" {
		t.Errorf("round robin on delta 1 failed, got %v", host)
	}
	host = w.NextHost()
	if host != "alpha" {
		t.Errorf("round robin on alpha 2 failed, got %v", host)
	}
}

func TestRoundRobinDefault(t *testing.T) {
	viper.Reset()
	w := &worker.ElasticSearchWorker{}
	w.Init()
	host := w.NextHost()
	if host != "localhost" {
		t.Errorf("round robin on localhost 1 failed, got %v", host)
	}
	host = w.NextHost()
	if host != "localhost" {
		t.Errorf("round robin on localhost 2 failed, got %v", host)
	}
	host = w.NextHost()
	if host != "localhost" {
		t.Errorf("round robin on localhost 3 failed, got %v", host)
	}
}

func TestInit(t *testing.T) {
	viper.Reset()
	w := &worker.ElasticSearchWorker{}
	err := w.Init()
	if err != nil {
		t.Errorf("expected everything to be fine on Init(), but this error: %v", err)
	} else {
		if w.CurrentCount() != 0 {
			t.Errorf("expected initial count to be 0, but is %v", w.CurrentCount())
		}
		if len(w.CurrentItems()) != worker.ConfiguredElasticSearchMax()*2 {
			t.Errorf("expected initial current items to be %v, but size is %v", worker.ConfiguredElasticSearchMax()*2, len(w.CurrentItems()))
		}
		if w.Endpoint() != "http://localhost:9200/_bulk" {
			t.Errorf("expected %v, got %v", "http://localhost:9200/_bulk", w.Endpoint())
		}
		if w.Index() != "analytics" {
			t.Errorf("expected %v, got %v", "analytics", w.Index())
		}
		if w.DocumentType() != "event" {
			t.Errorf("expected %v, got %v", "event", w.DocumentType())
		}
		if w.FlushEvery() != 10000 {
			t.Errorf("expected %v, got %v", 10000, w.FlushEvery())
		}
		if w.Mocking() != false {
			t.Errorf("expected %v, got %v", false, w.Mocking())
		}
		if w.UseDateSuffix() != false {
			t.Errorf("expected %v, got %v", false, w.UseDateSuffix())
		}
	}
}
