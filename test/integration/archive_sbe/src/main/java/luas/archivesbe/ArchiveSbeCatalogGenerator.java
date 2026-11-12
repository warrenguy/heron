package luas.archivesbe;

import java.io.IOException;
import java.nio.file.Path;

/** Rewrites catalog.json hex from the archive control encoders. */
public final class ArchiveSbeCatalogGenerator {
    private ArchiveSbeCatalogGenerator() {
    }

    public static void main(final String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: ArchiveSbeCatalogGenerator catalog.json");
        }
        ArchiveSbeCatalog.regenerate(Path.of(args[0]));
    }
}
