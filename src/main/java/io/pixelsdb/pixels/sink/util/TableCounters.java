package io.pixelsdb.pixels.sink.util;

/**
 * Inner class to hold and manage per-table transaction row counts.
 */
public class TableCounters
{
    // currentCount is volatile for visibility across threads, as it's incremented during writeRow.
    private volatile int currentCount = 0;
    private final int totalCount;           // The expected total number of rows

    public TableCounters(int totalCount)
    {
        this.totalCount = totalCount;
    }

    public void increment()
    {
        currentCount++;
    }

    public boolean isComplete()
    {
        // Checks if the processed count meets or exceeds the expected total count.
        return currentCount >= totalCount;
    }

    public int getCurrentCount()
    {
        return currentCount;
    }

    public int getTotalCount()
    {
        return totalCount;
    }
}
