/**
 * Copyright (C) 2011 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.entur.kishar.gtfsrt;

class TripAndVehicleKey {

  private final String tripId;

  private final String serviceDate;

  private final String vehicleId;

  public static TripAndVehicleKey fromTripIdServiceDateAndVehicleId(String tripId, String serviceDate, String vehicleId) {
    return new TripAndVehicleKey(tripId, serviceDate, vehicleId);
  }

  private TripAndVehicleKey(String tripId, String serviceDate, String vehicleId) {
    this.tripId = tripId;
    this.serviceDate = serviceDate;
    this.vehicleId = vehicleId;
  }

  public String getTripId() {
    return tripId;
  }

  public String getServiceDate() {
    return serviceDate;
  }

  public String getVehicleId() {
    return vehicleId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((serviceDate == null) ? 0 : serviceDate.hashCode());
    result = prime * result + ((tripId == null) ? 0 : tripId.hashCode());
    result = prime * result
        + ((vehicleId == null) ? 0 : vehicleId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TripAndVehicleKey other = (TripAndVehicleKey) obj;
    if (serviceDate == null) {
      if (other.serviceDate != null)
        return false;
    } else if (!serviceDate.equals(other.serviceDate))
      return false;
    if (tripId == null) {
      if (other.tripId != null)
        return false;
    } else if (!tripId.equals(other.tripId))
      return false;
    if (vehicleId == null) {
      if (other.vehicleId != null)
        return false;
    } else if (!vehicleId.equals(other.vehicleId))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "tripId=" + tripId + " serviceDate=" + serviceDate + " vehicleId="
        + vehicleId;
  }

}
