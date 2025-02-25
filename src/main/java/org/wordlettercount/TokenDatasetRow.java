package org.wordlettercount;

import java.io.Serializable;

/**
     * Class for defining rows in streamlined approach.
     * Spark uses this with Encoder to be able to iterate over rows and convert to DataFrame.
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