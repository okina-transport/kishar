package org.entur.kishar.gtfsrt;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class GtfsTripsServiceTest {

    public static final File NON_EXISTING_DIRECTORY = new File("non-existing-file");
    public static final File TRIPS_WITH_NO_DATASET = new File("src/test/resources/GtfsTripsServiceTest/tripsWithNoDataset");
    public static final File TRIPS_WITH_EMPTY_DATASET = new File("src/test/resources/GtfsTripsServiceTest/tripsWithEmptyDataset");
    public static final File TRIPS_WITH_EMPTY_TRIPS_FILE = new File("src/test/resources/GtfsTripsServiceTest/tripsWithEmptyTripsFile");
    public static final File TRIPS_WITH_INVALID_TRIPS_FILE = new File("src/test/resources/GtfsTripsServiceTest/tripsWithInvalidTripsFile");
    public static final File TRIPS_DATASET_WITH_MULTIPLE_TRIPS_FILE = new File("src/test/resources/GtfsTripsServiceTest/tripsDatasetWithMultipleTripsFile");
    public static final File TRIPS_ACCEPTANCE = new File("src/test/resources/GtfsTripsServiceTest/tripsAcceptance");

    @Test
    public void test_loadTripsFromFileSystem_whenTripsDirectoryDoesNotExist_mapIsEmpty() {
        // Arrange
        GtfsTripsService tested = new GtfsTripsService(NON_EXISTING_DIRECTORY);

        // Act
        tested.loadTripsFromFileSystem();

        // Assert
        Assertions.assertFalse(NON_EXISTING_DIRECTORY.exists(), "trips directory should not exist");
        Assertions.assertTrue(MapUtils.isEmpty(tested.getTripIdsByDatasetId()), "map should be empty");
    }

    @Test
    public void test_loadTripsFromFileSystem_whenThereIsNotDatasetDirectoriesInTripsDirectory_mapIsEmpty() {
        // Arrange
        GtfsTripsService tested = new GtfsTripsService(TRIPS_WITH_NO_DATASET);

        // Act
        tested.loadTripsFromFileSystem();

        // Assert
        Assertions.assertTrue(ArrayUtils.isEmpty(TRIPS_WITH_NO_DATASET.list()), "trips directory should have no subdirectory");
        Assertions.assertTrue(MapUtils.isEmpty(tested.getTripIdsByDatasetId()), "map should be empty");
    }

    @Test
    public void test_loadTripsFromFileSystem_whenDatasetDirectoryContainsNoTripsTxtFile_discardDataset() {
        // Arrange
        GtfsTripsService tested = new GtfsTripsService(TRIPS_WITH_EMPTY_DATASET);

        // Act
        tested.loadTripsFromFileSystem();

        // Assert
        Assertions.assertFalse(tested.getTripIdsByDatasetId().containsKey("DATASET"), "should discard dataset");
    }

    @Test
    public void test_loadTripsFromFileSystem_whenDatasetDirectoryContainsEmptyTripsTxtFile_discardDataset() {
        // Arrange
        GtfsTripsService tested = new GtfsTripsService(TRIPS_WITH_EMPTY_TRIPS_FILE);

        // Act
        tested.loadTripsFromFileSystem();

        // Assert
        Assertions.assertFalse(tested.getTripIdsByDatasetId().containsKey("DATASET"), "should discard dataset");
    }

    @Test
    public void test_loadTripsFromFileSystem_whenDatasetDirectoryContainsInvalidTripsTxtFile_discardDataset() {
        // Arrange
        GtfsTripsService tested = new GtfsTripsService(TRIPS_WITH_INVALID_TRIPS_FILE);

        // Act
        tested.loadTripsFromFileSystem();

        // Assert
        Assertions.assertFalse(tested.getTripIdsByDatasetId().containsKey("DATASET"), "should discard dataset");
    }


    @Test
    public void test_loadTripsFromFileSystem_whenDatasetContainsMultipleTripsTxtFile_readsMostRecentFile() {
        // Arrange
        GtfsTripsService tested = new GtfsTripsService(TRIPS_DATASET_WITH_MULTIPLE_TRIPS_FILE);

        // Act
        tested.loadTripsFromFileSystem();

        // Assert
        var tripsFilenames = TRIPS_DATASET_WITH_MULTIPLE_TRIPS_FILE.listFiles()[0].list();
        Arrays.sort(tripsFilenames);
        Assertions.assertArrayEquals(
                new String[]{"trips_20250326000000.txt", "trips_20250326133311.txt", "trips_20250326235959.txt"},
                tripsFilenames, "dataset directory should contain multiple trips files");
        Assertions.assertEquals(Map.of("DATASET", Set.of("3")), tested.getTripIdsByDatasetId(), "should read most recent trip file");
    }

    @Test
    public void test_loadTripsFromFileSystem_acceptance() {
        // Arrange
        GtfsTripsService tested = new GtfsTripsService(TRIPS_ACCEPTANCE);

        // Act
        tested.loadTripsFromFileSystem();

        // Assert
        Assertions.assertEquals(
                Map.of("DATASET1", Set.of("azer", "tyui", "opqs", "dfgh", "jklm", "wxcv", "bn"),
                        "DATASET2", Set.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"),
                        "DATASET3", Set.of("quentin", "de", "montargis")
                ), tested.getTripIdsByDatasetId(), "should pass acceptance test");
    }

    @Test
    public void test_existsTripByDatasetIdAndTripId_whenDatasetNotInCache_doesNotExistInCache() {
        // Arrange
        GtfsTripsService tested = new GtfsTripsService(TRIPS_WITH_NO_DATASET);
        tested.getTripIdsByDatasetId().clear();
        tested.getTripIdsByDatasetId().put("DATASET", Set.of("A"));

        // Act
        boolean result = tested.existsTripByDatasetIdAndTripId("DATASET1", "A");

        // Assert
        Assertions.assertFalse(result, "should not exist when dataset is not in cache");
    }

    @Test
    public void test_existsTripByDatasetIdAndTripId_whenTripIdNotInCache_doesNotExistInCache() {
        // Arrange
        GtfsTripsService tested = new GtfsTripsService(TRIPS_WITH_NO_DATASET);
        tested.getTripIdsByDatasetId().clear();
        tested.getTripIdsByDatasetId().put("DATASET", Set.of("A"));

        // Act
        boolean result = tested.existsTripByDatasetIdAndTripId("DATASET", "B");

        // Assert
        Assertions.assertFalse(result, "should not exist when tripId is not in cache");
    }

    @Test
    public void test_existsTripByDatasetIdAndTripId_whenTripIdInCache_existsInCache() {
        // Arrange
        GtfsTripsService tested = new GtfsTripsService(TRIPS_WITH_NO_DATASET);
        tested.getTripIdsByDatasetId().clear();
        tested.getTripIdsByDatasetId().put("DATASET", Set.of("A"));

        // Act
        boolean result = tested.existsTripByDatasetIdAndTripId("DATASET", "A");

        // Assert
        Assertions.assertTrue(result, "should exist when tripId is in cache");
    }

}
