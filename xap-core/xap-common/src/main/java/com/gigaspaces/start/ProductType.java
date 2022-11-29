package com.gigaspaces.start;

/**
 * @author Niv Ingberg
 * @since 12.2
 */
public enum ProductType {
    XAP, InsightEdge;

    private final String nameLowerCase = this.name().toLowerCase();

    public String getNameLowerCase() {
        return nameLowerCase;
    }

    /**
     * Modified name of XAP is smart-cache and is used as
     * - zip file prefix
     * - chart prefix
     * - image repo prefix
     *
     * @return A Lower case modified name ('smart-cache' instead of 'xap'; 'insightedge' stays the same
     * @since 16.3
     */
    public String toModifiedName() {
        return this.equals(XAP) ? "smart-cache" : "insightedge";
    }
}
