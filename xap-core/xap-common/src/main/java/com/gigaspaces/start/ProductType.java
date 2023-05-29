package com.gigaspaces.start;

/**
 * @author Niv Ingberg
 * @since 12.2
 */
public enum ProductType {
    XAP("xap"), InsightEdge("insightedge"), SmartCache("smart-cache");

    private final String nameLowerCase;

    ProductType(String nameLowerCase) {
        this.nameLowerCase = nameLowerCase;
    }

    public String getNameLowerCase() {
        return nameLowerCase;
    }
}
