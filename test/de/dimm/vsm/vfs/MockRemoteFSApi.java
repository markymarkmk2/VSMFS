/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.interfaces.RemoteFSApi;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Administrator
 */
public class MockRemoteFSApi implements RemoteFSApi {

    VfsBufferedFileTest test;

    public MockRemoteFSApi( VfsBufferedFileTest test ) {
        this.test = test;
    }
            
    
        Map<Long,File> map = new ConcurrentHashMap<Long, File>();
        long handleNo = 1;
        
        @Override
        public RemoteFSElem create_fse_node( String fileName, String type ) throws IOException, PoolReadOnlyException, PathResolveException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public RemoteFSElem resolve_node( String path ) throws SQLException {
            
            return new RemoteFSElem( new File(path));
        }

        @Override
        public long getTotalBlocks() {
            return 10000;
        }

        @Override
        public long getUsedBlocks() {
            return 10000;
        }

        @Override
        public int getBlockSize() {
            return 1000*1000;
        }

        @Override
        public void mkdir( String pathName ) throws IOException, PoolReadOnlyException, PathResolveException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public String getName() {
            return "MockHandler";
        }

        @Override
        public boolean remove_fse_node( String path ) throws IOException, PoolReadOnlyException, SQLException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public boolean remove_fse_node_idx( long idx ) throws IOException, PoolReadOnlyException, SQLException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public List<RemoteFSElem> get_child_nodes( RemoteFSElem handler ) throws SQLException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void move_fse_node( String from, String to ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void move_fse_node_idx( long from, String to ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public long open_file_handle_no( RemoteFSElem node, boolean create ) throws SQLException, IOException, PoolReadOnlyException, PathResolveException {
            File f = new File(node.getPath());
            long h = ++handleNo;
            map.put(h, f);            
            return h;
        }

        @Override
        public boolean exists( RemoteFSElem fseNode ) throws IOException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public boolean isReadOnly( long idx ) throws IOException, SQLException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void set_ms_times( long idx, long ctime, long atime, long mtime ) throws IOException, SQLException, PoolReadOnlyException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void force( long idx, boolean b ) throws IOException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public byte[] read( long idx, int length, long offset ) throws IOException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
        File get( long handleNo ){
            File f = map.get(handleNo);
            if (f == null)
                throw new RuntimeException("Unbekannter Handle " + handleNo); 
            return f;
        }

        @Override
        public void close( long idx ) throws IOException {
            
            File f = map.remove(handleNo);
            if (f == null)
                throw new RuntimeException("Unbekannter Handle " + handleNo); 
        }

        @Override
        public void create( long idx ) throws IOException, PoolReadOnlyException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void truncateFile( long idx, long size ) throws IOException, SQLException, PoolReadOnlyException {
            test.log.debug("truncateFile " + size);
        }

        @Override
        public void writeFile( long idx, byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException {
            File f = map.remove(handleNo);
            if (f == null)
                throw new RuntimeException("Unbekannter Handle " + handleNo); 
            //log.debug("writeFile " + offset + " length " + length);
        }

        @Override
        public void writeBlock( long idx, String hash, byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException {
            //log.debug("writeBlock " + offset + " length " + length + " hash: " + hash);
           int writeBlockSize = test.getWriteBlockSize();
            int rel = (int)(offset % writeBlockSize);
            
            for (int i = 0; i < length; i++)
            {
                int x = (i + rel) % writeBlockSize;
                
                if ((byte)(x & 0xff) != b[i])
                    throw new RuntimeException("Data failure");
            }            
           
        }

        @Override
        public void setAttribute( RemoteFSElem elem, String string, Integer valueOf ) throws IOException, SQLException, PoolReadOnlyException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public String readSymlink( RemoteFSElem elem ) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void createSymlink( RemoteFSElem elem, String to ) throws IOException, SQLException, PoolReadOnlyException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public String getXattribute( RemoteFSElem elem, String name ) throws SQLException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public List<String> listXattributes( RemoteFSElem elem ) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void addXattribute( RemoteFSElem elem, String name, String valStr ) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void updateElem( RemoteFSElem elem, long handleNo ) throws IOException, SQLException, PoolReadOnlyException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
    }