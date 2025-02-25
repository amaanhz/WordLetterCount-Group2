package org.wordlettercount;

import java.io.Serializable;

/**
     * Class for defining rows in streamlined approach.
     */
public class TokenDatasetRow implements Serializable {
    private String token;
    private String type;
    public TokenDatasetRow() {}
    public TokenDatasetRow(String token, String type) { this.token = token; this.type = type; }
    public String getToken() { return this.token; }
    public String getType() { return this.type; }
    public void setToken(String token) { this.token = token; }
    public void setType(String type) { this.type = type; }
}