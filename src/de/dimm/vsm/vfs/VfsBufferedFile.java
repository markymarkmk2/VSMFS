/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.Utilities.CryptTools;
import de.dimm.vsm.fsutils.RemoteStoragePoolHandler;
import de.dimm.vsm.net.RemoteFSElem;
import fr.cryptohash.Digest;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;








class VfsBlock
{
    long offset;
    int len;
    byte[] data;
    boolean dirtyWrite;
    int validLen;
    long lastTS;
    String hash;
    

    public VfsBlock( long offset, int len, byte[] data )
    {
        this.offset = offset;
        this.len = len;
        this.validLen = len;
        this.data = data;
        touchTS();
    }

    public VfsBlock( long offset, int len, int validLen, byte[] data )
    {
        this(offset, len, data);        
        this.validLen = validLen;
    }

     
    public void setDirtyWrite( boolean dirtyWrite )
    {
        this.dirtyWrite = dirtyWrite;
    }

    public boolean isDirtyWrite()
    {
        return dirtyWrite;
    }

    boolean isComplete()
    {
        return validLen == len;
    }
    final void touchTS()
    {
        lastTS = System.currentTimeMillis();
    }
}




/**
 *
 * @author root
 */
public class VfsBufferedFile extends VfsFile
{
    Map<Long,VfsBlock> blockMap;
    int blockSize = 1024*1024;
    private static final int MAX_DIRTY_BLOCKS = 20;
    
    private static final int MAX_BLOCK_LIMIT = 50;
    private static final int MIN_BLOCK_LIMIT = 10;
    
    // CREATE URLSAFE ENCODE
    Digest digest = new fr.cryptohash.SHA1();
    
    
    public VfsBufferedFile( IVfsDir parent, String path, RemoteFSElem elem, RemoteStoragePoolHandler remoteFSApi )
    {
        super( parent, path, elem, remoteFSApi );
        blockMap = new HashMap<>(1);
    }
    
    void workBlockCache()
    {
        // Clear BlockCache on Read
        if (blockMap.size() > MAX_BLOCK_LIMIT)
        {
            Collection<VfsBlock> blocks = blockMap.values();
            List<VfsBlock> list = new ArrayList<>(blocks);
            Collections.sort(list, new Comparator<VfsBlock>() {

                @Override
                public int compare( VfsBlock o1, VfsBlock o2 )
                {
                    return (int)(o1.lastTS - o2.lastTS);
                }
            });
            
            for (int i = 0; i < list.size(); i++ )
            {
                long offset = list.get(i).offset;
                VfsBlock block = blockMap.get(offset);
                if (!block.isDirtyWrite())
                {
                    blockMap.remove(offset);                
                    if (blockMap.size() < MIN_BLOCK_LIMIT)
                        break;
                }
            }
        }
    }

    @Override
    public byte[] read( long handleNo, long offset, int len ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        checkValidOpen(handleNo, false);
        
        if (offset > getSize())
            return null;
        
        VfsBlock block = getBlock(offset);
        if (block == null)
        {
            block = readBlock( handleNo, offset );
        }
        
        block.touchTS();
        
        // Block is complete and fits?
        if (block.validLen == len && block.isComplete())
            return block.data;

        // readpos in Block
        int offsetInBlock = (int)(offset - block.offset);            
        int readLen = len;
        // Calc readable len of data
        if (offsetInBlock + readLen > block.validLen)
        {
            readLen = block.validLen - offsetInBlock;
        }
        // read data
        byte[] data = new byte[readLen];
        System.arraycopy( block.data, offsetInBlock, data, 0, readLen);
        return data;
    }

    @Override
    public void write( long handleNo, long offset, int len, byte[] data ) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        checkValidOpen(handleNo, true);
        
        int writtenLen = 0;
        while(writtenLen < len)
        {
            long realOffset = offset + writtenLen;
            int realLen = len - writtenLen;
            VfsBlock block = getBlock(realOffset);
            if (block == null)
            {
                // Read all of existing block;
                if (!isNewFile() && getNode().getDataSize() > offset)
                {                    
                    try
                    {
                        block = readBlock(handleNo, offset);
                    }
                    catch (IOException | SQLException iOException)
                    {
                        debug( "Keine Daten bei Blockadresse " + offset + ": " + iOException);
                    }                    
                }

                // No Data on Server
                if (block == null)
                {
                    // Create new Block on Blockboundary
                    long blockOffset = getBlockOffset(realOffset);

                    byte[] blockData = new byte[blockSize];
                    block = new VfsBlock( blockOffset, blockSize, 0, blockData );    
                    blockMap.put( blockOffset, block);
                }
            }

            int offsetInBlock = (int)(realOffset - block.offset);   

            // Schreiben über Block hinaus?
            if (realLen > block.len - offsetInBlock)
            {
                // Block ist zu klein?
                if (block.len < blockSize)
                {
                    // Create new Block on Blockboundary
                    long blockOffset = getBlockOffset(offset);
                    byte[] blockData = new byte[blockSize];

                    // Kopiere bestehende Daten hinein
                    System.arraycopy( block.data, 0, blockData, 0, block.len);
                    block = new VfsBlock( blockOffset, blockSize, 0, blockData );    
                    blockMap.put( blockOffset, block);
                }
                // Korrigiere tatsaechlich moegliche Laenge
                if (realLen > block.len - offsetInBlock)
                {
                    realLen = block.len - offsetInBlock;
                }
            }

            System.arraycopy( data, 0, block.data, offsetInBlock, realLen);

            // Block hat neue Daten
            block.setDirtyWrite( true );

            // Neues Blockende prüfen
            if (block.validLen < offsetInBlock + realLen)
            {
                block.validLen = offsetInBlock + realLen;
            }
            block.touchTS();

            // Geschriebene Mange mitführen
            writtenLen += realLen;
        }
                
        if (getSize() < offset + len)
        {
            getNode().setDataSize( offset + len );
            setUpdatedFileSize(true);
        }
        checkFlushBlocks(handleNo);
    }

    @Override
    public void close(long handleNo) throws IOException
    {
        try
        {
            // Flush pending writes
            writeUnwrittenBlocks(handleNo);
            // Check wether Filesize has changed
            if (isUpdatedFileSize()) 
            {
                debug( "Korrigiere neue Länge von " + getNode().getName() + " -> " + getNode().getDataSize());
                truncate(handleNo, getNode().getDataSize());
            }
        }
        catch (PoolReadOnlyException | SQLException | PathResolveException ex)
        {
            throw new IOException( "Fehler bei close: " + ex.getMessage(), ex);
        }
        super.close(handleNo);
        
        // TODO: cache a la ehCache with blocks
        debug( "Clearing "+ blockMap.size() + " Blocks ");
        blockMap.clear();
        
    }
    
    @Override
    public void flush(long handleNo) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        try
        {
            writeUnwrittenBlocks(handleNo);
        }
        catch (PoolReadOnlyException | SQLException | PathResolveException ex)
        {
            throw new IOException( "Fehler bei flush: " + ex.getMessage(), ex);
        }
        super.flush(handleNo);
    }
    
    
    private VfsBlock readBlock(long handleNo, long offset) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        workBlockCache();
                
        long blockOffset = getBlockOffset(offset);
        int blockLen = blockSize;
        if (blockOffset + blockLen > getSize())
        {
            blockLen = (int)(getSize() - blockOffset);
        }
        debug( "Read Block " + blockOffset + " len " + blockLen);
        byte[] data =  super.read( handleNo, blockOffset, blockLen );

        VfsBlock block = new VfsBlock( blockOffset, data.length, data);
        blockMap.put( blockOffset, block);
        return block;
    }
    
    private long getBlockOffset(long len)
    {
        return (len/blockSize) * blockSize;
    }
    
    private VfsBlock getBlock( long offset)
    {
        VfsBlock block = blockMap.get( getBlockOffset(offset) );        
        if (block != null)
        {
            return block;
        }        
        return null;
    }

    private int cntDirtyBlocks()
    {
        int dirtyBlocks = 0;
        for (Map.Entry<Long, VfsBlock> entry : blockMap.entrySet())
        {
            VfsBlock vfsBlock = entry.getValue();
            if (vfsBlock.isDirtyWrite())
                dirtyBlocks++;            
        }
        return dirtyBlocks;
    }
    private void checkFlushBlocks(long handleNo) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        int dirtyBlocks = cntDirtyBlocks();
        
        if (dirtyBlocks < MAX_DIRTY_BLOCKS)
            return;

        flushBlocks(handleNo, dirtyBlocks/2);
 
    }
    private void writeUnwrittenBlocks(long handleNo) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        Collection<VfsBlock> blocks = blockMap.values();
        List<VfsBlock> list = new ArrayList<>(blocks);
        Collections.sort(list, new Comparator<VfsBlock>() {

            @Override
            public int compare( VfsBlock o1, VfsBlock o2 )
            {
                return (int)(o1.lastTS - o2.lastTS);
            }
        });
        
       for (VfsBlock vfsBlock : list)
        {
            if (vfsBlock.isDirtyWrite())
            {
                debug( "Write Block " + vfsBlock.offset + " len " + vfsBlock.validLen);
                write_block( handleNo, vfsBlock);
                vfsBlock.setDirtyWrite(false);
                touchMTime();
                vfsBlock.touchTS();
            }
        }
    }
    
    void flushBlocks( long handleNo, int maxBlocksToFlush) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        debug( "flushBlocks " + maxBlocksToFlush + " of " + blockMap.size());

        Collection<VfsBlock> blocks = blockMap.values();
        List<VfsBlock> list = new ArrayList<>(blocks);
        Collections.sort(list, new Comparator<VfsBlock>() {

            @Override
            public int compare( VfsBlock o1, VfsBlock o2 )
            {
                long diff = (o1.lastTS - o2.lastTS);
                if (diff == 0)
                {
                    diff = (o1.offset- o2.offset);
                }
                return Long.signum(diff);
            }
        });
        
        for (VfsBlock vfsBlock : list)
        {            
            if (vfsBlock.isDirtyWrite() && vfsBlock.isComplete())
            {
                write_block( handleNo, vfsBlock);
                vfsBlock.setDirtyWrite(false);
                touchMTime();
                vfsBlock.touchTS();
                
                maxBlocksToFlush--;
                if (maxBlocksToFlush <= 0)
                    break;
            }
        }
       
        if (blockMap.size() > MAX_BLOCK_LIMIT)
        {
            for (int i = 0; i < list.size() - MIN_BLOCK_LIMIT; i++ )
            {
                if (!list.get(i).isDirtyWrite())
                {
                    long offset = list.get(i).offset;
                    blockMap.remove(offset);
                }
            }
        }       
    }
    
    void write_block(long handleNo, VfsBlock vfsBlock) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        // TODO: calc Hashes, send to Server, and if necessary put hashes into blockbuffer for server to fetch.
        byte[] hash = digest.digest(vfsBlock.data);
        vfsBlock.hash = CryptTools.encodeUrlsafe(hash);
        
        debug( "Write Block " + vfsBlock.offset + " len " + vfsBlock.validLen);   
        
        remoteFSApi.writeBlock(handleNo, vfsBlock.hash, vfsBlock.data, vfsBlock.validLen, vfsBlock.offset);
        
        if (getSize() < vfsBlock.offset + vfsBlock.validLen)
        {
            getNode().setDataSize( vfsBlock.offset + vfsBlock.validLen);
            setUpdatedFileSize(true);
        }        
    }
}
