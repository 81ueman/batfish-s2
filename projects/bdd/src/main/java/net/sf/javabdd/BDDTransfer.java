package net.sf.javabdd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Serializes a {@link BDD} to a portable Base64 string and reconstructs it in another {@link
 * JFactory}. Used by S2 to forward symbolic packets across workers.
 *
 * <p>Representation: the set of reachable nodes, post-order, as (index, level, low, high). Levels
 * (not variable indices) are stored because {@link JFactory#bdd_makenode} takes a level; workers use
 * the same variable order, so this round-trips faithfully.
 */
public class BDDTransfer {

  private static final String MAGIC = "FORMAT:S2.BDD";

  public String save(BDD bdd) throws IOException {
    JFactory manager = (JFactory) bdd.getFactory();
    int root = bdd.getIndex();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(new GZIPOutputStream(baos))) {
      out.writeUTF(MAGIC);
      out.writeInt(manager.varNum());
      out.writeInt(root);
      saveRec(manager, out, new HashSet<>(), root);
      out.writeInt(-1);
    }
    return Base64.getEncoder().encodeToString(baos.toByteArray());
  }

  private static void saveRec(JFactory manager, DataOutputStream out, Set<Integer> visited, int bdd)
      throws IOException {
    if (bdd < 2 || !visited.add(bdd)) {
      return;
    }
    int level = manager.LEVEL(bdd);
    int low = manager.LOW(bdd);
    int high = manager.HIGH(bdd);
    saveRec(manager, out, visited, low);
    saveRec(manager, out, visited, high);
    out.writeInt(bdd);
    out.writeInt(level);
    out.writeInt(low);
    out.writeInt(high);
  }

  public BDD load(JFactory manager, String serialized) throws IOException {
    byte[] bytes = Base64.getDecoder().decode(serialized);
    try (DataInputStream in =
        new DataInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)))) {
      if (!MAGIC.equals(in.readUTF())) {
        throw new IOException("Not an S2 BDD payload");
      }
      int varNum = in.readInt();
      if (manager.varNum() < varNum) {
        manager.setVarNum(varNum);
      }
      int target = in.readInt();
      Map<Integer, Integer> map = new HashMap<>();
      map.put(0, 0);
      map.put(1, 1);
      while (true) {
        int name = in.readInt();
        if (name == -1) {
          break;
        }
        int level = in.readInt();
        Integer low = map.get(in.readInt());
        Integer high = map.get(in.readInt());
        if (low == null || high == null) {
          throw new IOException("Corrupt BDD payload: unknown child node");
        }
        int node = manager.bdd_makenode(level, low, high);
        manager.INCREF(node);
        map.put(name, node);
      }
      Integer newTarget = map.get(target);
      if (newTarget == null) {
        throw new IOException("Corrupt BDD payload: unknown root node");
      }
      return manager.makeBDD(newTarget);
    }
  }
}
