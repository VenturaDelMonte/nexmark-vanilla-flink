package io.ventura.nexmark.beans;

public class Query8WindowOutput {

    private final Long windowEvictingTimestamp;
    private final Long personCreationTimestamp;
    private final Long personIngestionTimestamp;
    private final Long auctionCreationTimestamp;
    private final Long auctionIngestionTimestamp;
    private final long personId;

    public Query8WindowOutput(Long windowEvictingTimestamp,
                              Long personCreationTimestamp,
                              Long personIngestionTimestamp,
                              Long auctionCreationTimestamp,
                              Long auctionIngestionTimestamp,
                              long personId) {
        this.windowEvictingTimestamp = windowEvictingTimestamp;
        this.personCreationTimestamp = personCreationTimestamp;
        this.personIngestionTimestamp = personIngestionTimestamp;
        this.auctionCreationTimestamp = auctionCreationTimestamp;
        this.auctionIngestionTimestamp = auctionIngestionTimestamp;
        this.personId = personId;
    }

    public Long getAuctionCreationTimestamp() {
        return auctionCreationTimestamp;
    }

    public Long getPersonCreationTimestamp() {
        return personCreationTimestamp;
    }

    public Long getPersonIngestionTimestamp() {
        return personIngestionTimestamp;
    }

    public Long getAuctionIngestionTimestamp() {
        return auctionIngestionTimestamp;
    }

    public long getPersonId() {
        return personId;
    }

    public Long getWindowEvictingTimestamp() {
        return windowEvictingTimestamp;
    }
}