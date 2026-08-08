package za.co.fnb.dcre.prg.service;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Streaming stage-then-rename boundary write with StagedWrite's R-24
 * semantics at 30M-tx scale: tmp in the SAME directory, header + TX lines
 * streamed slice by slice through a BufferedWriter (the whole-book render
 * into a List of lines blew the heap), trailer + ATOMIC_MOVE on
 * {@link #commit()}. Callers check target existence BEFORE {@link #begin}
 * (existing target = prior emission, restart no-op), exactly like
 * platform-files StagedWrite, which stays untouched. Local to PRG
 * deliberately; promoting this shape to platform-files is a post-sweep
 * refactor candidate (SCRUM-42).
 */
final class StreamedPsrWrite implements Closeable {

    private final Path target;
    private final Path tmp;
    private final BufferedWriter out;
    private long count;
    private boolean committed;

    private StreamedPsrWrite(final Path target, final Path tmp, final BufferedWriter out) {
        this.target = target;
        this.tmp = tmp;
        this.out = out;
    }

    /** Opens the tmp stream (stale tmp from a crashed prior attempt deleted first) and writes the header. */
    static StreamedPsrWrite begin(final Path target, final String header) throws IOException {
        Files.createDirectories(target.getParent());
        final Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.deleteIfExists(tmp);
        final StreamedPsrWrite write = new StreamedPsrWrite(target, tmp, Files.newBufferedWriter(tmp));
        try {
            write.line(header);
        } catch (final IOException e) {
            write.close();
            throw e;
        }
        return write;
    }

    /**
     * Reads a standing PSR's trailer line via a bounded tail read (never the
     * whole file: delta PSRs reach tens of millions of lines). The trailer is
     * "END|" + count, so 64 bytes always cover it plus the final newline.
     * SCRUM-55 kill-resume classifier: END|0 identifies a heartbeat file even
     * when its registry row is missing (a real delta file counts >= 1 TX line).
     */
    static String trailer(final Path target) throws IOException {
        try (SeekableByteChannel channel = Files.newByteChannel(target)) {
            final int tail = (int) Math.min(64, channel.size());
            channel.position(channel.size() - tail);
            final ByteBuffer buf = ByteBuffer.allocate(tail);
            while (buf.hasRemaining() && channel.read(buf) >= 0) {
                // bounded: at most `tail` bytes
            }
            final String[] lines = new String(buf.array(), 0, buf.position(), StandardCharsets.UTF_8)
                    .strip().split("\\R");
            return lines[lines.length - 1];
        }
    }

    void writeTx(final String txLine) throws IOException {
        line(txLine);
        count++;
    }

    /** Non-TX body line (heartbeat HB/PD): written verbatim, never counted in the END trailer. */
    void writeInfo(final String infoLine) throws IOException {
        line(infoLine);
    }

    long count() {
        return count;
    }

    /** Trailer with the streamed count, close, ATOMIC_MOVE: the target appears complete or not at all. */
    void commit() throws IOException {
        line("END|" + count);
        out.close();
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
        committed = true;
    }

    /** Abort path (exception before commit): never leaves a partial tmp behind. */
    @Override
    public void close() throws IOException {
        if (committed) {
            return;
        }
        out.close();
        Files.deleteIfExists(tmp);
    }

    private void line(final String text) throws IOException {
        out.write(text);
        out.newLine();
    }
}
