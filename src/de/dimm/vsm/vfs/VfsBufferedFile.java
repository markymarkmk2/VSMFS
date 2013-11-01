/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.fsutils.RemoteStoragePoolHandler;
import de.dimm.vsm.net.RemoteFSElem;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;








class VfsBlock
{
    long offset;
    int len;
    byte[] data;
    boolean dirtyWrite;
    int validLen;
    

    public VfsBlock( long offset, int len, byte[] data )
    {
        this.offset = offset;
        this.len = len;
        this.validLen = len;
        this.data = data;
    }

    public VfsBlock( long offset, int len, int validLen, byte[] data )
    {
        this.offset = offset;
        this.len = len;
        this.validLen = validLen;
        this.data = data;
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
}




/**
 *
 * @author root
 */
public class VfsBufferedFile extends VfsFile
{
    Map<Long,VfsBlock> blockMap;
    int blockSize = 1024*1024;
    private static final int MAX_DIRTY_BLOCKS = 1;
    
    
    public VfsBufferedFile( IVfsDir parent, String path, RemoteFSElem elem, RemoteStoragePoolHandler remoteFSApi )
    {
        super( parent, path, elem, remoteFSApi );
        blockMap = new HashMap<>(1);
    }

    @Override
    public byte[] read( long offset, int len ) throws IOException, SQLException
    {
        if (offset > getSize())
            return null;
        
        VfsBlock block = getBlock(offset);
        if (block == null)
        {
            block = readBlock( offset );
        }
        
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
    public void write( long offset, int len, byte[] data ) throws IOException, SQLException, PoolReadOnlyException
    {
        int writtenLen = 0;
        while(writtenLen < len)
        {
            long realOffset = offset + writtenLen;
            int realLen = len - writtenLen;
            VfsBlock block = getBlock(realOffset);
            if (block == null)
            {
                // Read all of existing block;
                block = readBlock( offset);

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

            // Geschriebene Mange mitführen
            writtenLen += realLen;
        }
                
        if (getSize() < offset + len)
        {
            getNode().setDataSize( offset + len );
        }
        checkFlushBlocks();
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            flushBlocks();
        }
        catch (PoolReadOnlyException | SQLException ex)
        {
            throw new IOException( "Fehler bei close: " + ex.getMessage(), ex);
        }
        super.close();
        
        // TODO: cache a la ehCache with blocks
        debug( "Clearing "+ blockMap.size() + " Blocks ");
        blockMap.clear();
    }
    
    @Override
    public void flush() throws IOException
    {
        try
        {
            flushBlocks();
        }
        catch (PoolReadOnlyException | SQLException ex)
        {
            throw new IOException( "Fehler bei flush: " + ex.getMessage(), ex);
        }
        super.flush();
    }
    
    
    private VfsBlock readBlock(long offset) throws IOException, SQLException
    {
        long blockOffset = getBlockOffset(offset);
        int blockLen = blockSize;
        if (blockOffset + blockLen > getSize())
        {
            blockLen = (int)(getSize() - blockOffset);
        }
        debug( "Read Block " + blockOffset + " len " + blockLen);
        byte[] data =  super.read( blockOffset, blockLen );

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
    private void checkFlushBlocks() throws IOException, SQLException, PoolReadOnlyException
    {
        int dirtyBlocks = cntDirtyBlocks();
        
        if (dirtyBlocks < MAX_DIRTY_BLOCKS)
            return;

        flushBlocks(dirtyBlocks/2);
 
    }
    private void flushBlocks() throws IOException, SQLException, PoolReadOnlyException
    {
        flushBlocks(blockMap.size());
    }
    
    void flushBlocks( int maxBlocksToFlush) throws IOException, SQLException, PoolReadOnlyException
    {
       for (Map.Entry<Long, VfsBlock> entry : blockMap.entrySet())
        {
            VfsBlock vfsBlock = entry.getValue();
            if (maxBlocksToFlush <= 0)
                break;
            if (vfsBlock.isDirtyWrite())
            {
                debug( "Write Block " + vfsBlock.offset + " len " + vfsBlock.validLen);
                super.write( vfsBlock.offset, vfsBlock.validLen, vfsBlock.data );
                maxBlocksToFlush--;
                touchMTime();
            }
        }
    }
}
