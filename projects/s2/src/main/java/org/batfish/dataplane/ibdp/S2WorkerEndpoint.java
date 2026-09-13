// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.io.Serializable;
import java.util.Objects;

/** Address of a worker's S2 sidecar. */
public final class S2WorkerEndpoint implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String _host;
  private final int _port;

  public S2WorkerEndpoint(String host, int port) {
    _host = host;
    _port = port;
  }

  public String getHost() {
    return _host;
  }

  public int getPort() {
    return _port;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof S2WorkerEndpoint)) {
      return false;
    }
    S2WorkerEndpoint that = (S2WorkerEndpoint) o;
    return _port == that._port && _host.equals(that._host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_host, _port);
  }

  @Override
  public String toString() {
    return _host + ":" + _port;
  }
}
