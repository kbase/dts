package databases

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSecureHttpClient(t *testing.T) {
	client := SecureHttpClient(time.Second * 10)
	assert.Equal(t, time.Second*10, client.Timeout)
	assert.NotNil(t, client.Transport)

	secure_original_request := &http.Request{
		URL: &url.URL{
			Scheme: "https",
			Host:   "example.com",
			Path:   "/",
		},
	}
	insecure_original_request := &http.Request{
		URL: &url.URL{
			Scheme: "http",
			Host:   "example.com",
			Path:   "/",
		},
	}
	secure_redirect_target := &http.Request{
		URL: &url.URL{
			Scheme: "https",
			Host:   "redirect.com",
			Path:   "/",
		},
	}
	insecure_redirect_target := &http.Request{
		URL: &url.URL{
			Scheme: "http",
			Host:   "redirect.com",
			Path:   "/",
		},
	}

	// test secure to secure redirect
	err := client.CheckRedirect(secure_redirect_target, []*http.Request{secure_original_request})
	assert.Equal(t, http.ErrUseLastResponse, err)

	// test insecure to secure redirect
	err = client.CheckRedirect(secure_redirect_target, []*http.Request{insecure_original_request})
	assert.Equal(t, http.ErrUseLastResponse, err)

	// test secure to insecure redirect
	err = client.CheckRedirect(insecure_redirect_target, []*http.Request{secure_original_request})
	assert.IsType(t, &DowngradedRedirectError{}, err)
	dre := err.(*DowngradedRedirectError)
	assert.Equal(t, "redirect.com/", dre.Endpoint)

	// test insecure to insecure redirect
	// NOTE: this seems like it should be allowed, but for now it is not
	err = client.CheckRedirect(insecure_redirect_target, []*http.Request{insecure_original_request})
	assert.IsType(t, &DowngradedRedirectError{}, err)
	dre = err.(*DowngradedRedirectError)
	assert.Equal(t, "redirect.com/", dre.Endpoint)

}
