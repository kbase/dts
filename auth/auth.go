// Copyright (c) 2023 The KBase Project and its Contributors
// Copyright (c) 2023 Cohere Consulting, LLC
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is furnished to do
// so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package auth

// A record containing information about a DTS user using a DTS client to request file transfers.
type User struct {
	// name (human-readable and display-friendly)
	Name string
	// email address
	Email string
	// ORCID identifier associated with this user
	Orcid string
	// organization with which this user is affiliated
	Organization string
	// true if this user is a Superuser
	IsSuper bool
}

// A credential used for authorization and authentication
type Credential struct {
	// the ID used for authorization (username or UUID)
	Id string `yaml:"id"`
	// the secret used for authentication (e.g. password)
	// DO NOT STORE THIS IN A CONFIG FILE! Use an environment variable instead
	Secret string `yaml:"secret"`
}
