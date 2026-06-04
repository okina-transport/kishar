package org.entur.kishar.gtfsrt;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;
import org.entur.kishar.utils.CSVUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@Slf4j
@Getter
public class GtfsTripsService {



    public static final String CSV_HEADER_TRIP_ID = "trip_id";

    private final static FilenameFilter TRIPS_TXT_FILE_FILTER = (f, name) -> name.startsWith("trips") && name.endsWith(".txt");
    private final ConcurrentHashMap<String, Set<String>> tripIdsByDatasetId = new ConcurrentHashMap<>();
    private final File tripsDirectory;

    public GtfsTripsService(@Value("${kishar.gtfs.trips.directory:/tmp/trips}") File tripsDirectory) {
        this.tripsDirectory = tripsDirectory;
    }

    public void loadTripsFromFileSystem() {
        tripIdsByDatasetId.clear();
        if (!tripsDirectory.exists()) {
            log.warn("Trips directory '{}' does not exist", tripsDirectory);
            return;
        }
        File[] datasetDirectories = tripsDirectory.listFiles(File::isDirectory);
        if (datasetDirectories == null) {
            log.warn("No dataset directories found in '{}'", tripsDirectory);
            return;
        }
        for (var datasetDirectory : datasetDirectories) {
            File[] tripsTxtFiles = datasetDirectory.listFiles(TRIPS_TXT_FILE_FILTER);
            if (tripsTxtFiles == null || tripsTxtFiles.length == 0) {
                log.warn("No trips.txt file found in '{}', ignore this dataset", datasetDirectory);
                continue;
            }
            // trips file format is trips_yyyyMMddHHmmss.txt
            // get most recent trips.txt file
            var tripsTxtFile = Arrays.stream(tripsTxtFiles).max(Comparator.comparing(File::getName)).get();
            var gtfsTrips = parseTripsTxtFile(tripsTxtFile);
            if (CollectionUtils.isEmpty(gtfsTrips)) {
                log.warn("No trips found in '{}', ignore this dataset", tripsTxtFile);
                continue;
            }
            String datasetId = datasetDirectory.getName().toUpperCase();
            log.info("Parsed {} trips for dataset {}", gtfsTrips.size(), datasetId);
            tripIdsByDatasetId.put(datasetId, gtfsTrips);
        }
    }

    private Set<String> parseTripsTxtFile(File tripsTxtFile) {
        List<CSVRecord> records;
        try {
            records = CSVUtils.getRecords(tripsTxtFile);
        } catch (IOException e) {
            log.error("Error parsing CSV file {}", tripsTxtFile, e);
            return Set.of();
        }
        return records.stream().map(r -> r.get(CSV_HEADER_TRIP_ID)).collect(Collectors.toSet());
    }

    public boolean isDatasetInCache(String datasetId) {
        return tripIdsByDatasetId.containsKey(datasetId.toUpperCase());
    }

    public boolean existsTripByDatasetIdAndTripId(String datasetId, String tripId) {
        return tripIdsByDatasetId.containsKey(datasetId.toUpperCase()) && tripIdsByDatasetId.get(datasetId.toUpperCase()).contains(tripId);
    }

}
