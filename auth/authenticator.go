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

import (
	"bytes"
	//"crypto/aes"
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"

	"github.com/kbase/dts/config"
)

// This type accepts a valid access token in exchange for a user record. It is
// used as an additional method of authentication for the DTS. It's really a
// short-term solution, as the encrypted file is maintained manually, but it
// provides a method for adding DTS users without Acts of God.
type Authenticator struct {
	UserForToken map[string]User
}

func ReadAccessTokenFile(tokenFilePath string) (map[string]User, error) {
	//key := []byte(config.Service.Secret)

	encryptedText, err := os.ReadFile(tokenFilePath)
	if err != nil {
		return nil, err
	}

	//cipher, err := aes.NewCipher(key)
	//if err != nil {
	//	return nil, err
	//}

	//plainText := make([]byte, len(encryptedText))
	//cipher.Decrypt(plainText, encryptedText)
	plainText := []byte(encryptedText)

	// the plaintext content is a tab-delimited file with records like so:
	// Name\tEmail\tOrcid\tOrganization\tToken
	reader := csv.NewReader(bytes.NewReader(plainText))
	reader.Comma = '\t'
	reader.FieldsPerRecord = 5

	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	userRecords := make(map[string]User)
	for _, record := range records {
		token := record[4]
		userRecords[token] = User{
			Name:         record[0],
			Email:        record[1],
			Orcid:        record[2],
			Organization: record[3],
		}
	}

	return userRecords, nil
}

func NewAuthenticator() (*Authenticator, error) {
	var a Authenticator
	var err error
	filePath := filepath.Join(config.Service.DataDirectory, "access.dat")
	a.UserForToken, err = ReadAccessTokenFile(filePath)
	if err != nil {
		return nil, err
	}

	return &a, nil
}

// given an access token, returns a User or an error
func (a *Authenticator) GetUser(accessToken string) (User, error) {
	if user, found := a.UserForToken[accessToken]; found {
		return user, nil
	} else {
		return User{}, errors.New("Invalid access token!")
	}
}
