package io.ventura.nexmark.beans;

public class Query8WindowOutput {

    private final long windowEvictingTimestamp;
    private final long personCreationTimestamp;
    private final long personIngestionTimestamp;
    private final long auctionCreationTimestamp;
    private final long auctionIngestionTimestamp;
    private final long personId;

    public Query8WindowOutput(long windowEvictingTimestamp,
                              long personCreationTimestamp,
                              long personIngestionTimestamp,
                              long auctionCreationTimestamp,
                              long auctionIngestionTimestamp,
                              long personId) {
        this.windowEvictingTimestamp = windowEvictingTimestamp;
        this.personCreationTimestamp = personCreationTimestamp;
        this.personIngestionTimestamp = personIngestionTimestamp;
        this.auctionCreationTimestamp = auctionCreationTimestamp;
        this.auctionIngestionTimestamp = auctionIngestionTimestamp;
        this.personId = personId;
    }

    public long getAuctionCreationTimestamp() {
        return auctionCreationTimestamp;
    }

    public long getPersonCreationTimestamp() {
        return personCreationTimestamp;
    }

    public long getPersonIngestionTimestamp() {
        return personIngestionTimestamp;
    }

    public long getAuctionIngestionTimestamp() {
        return auctionIngestionTimestamp;
    }

    public long getPersonId() {
        return personId;
    }

    public long getWindowEvictingTimestamp() {
        return windowEvictingTimestamp;
    }
}