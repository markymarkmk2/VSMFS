/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

import de.dimm.vsm.Exceptions.PathResolveException;
import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.net.interfaces.RemoteFSApi;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;





/**
 *
 * @author root
 */
public class VfsBufferedFile extends VfsFile
{
    Map<Long,VfsBlock> blockMap;
    int blockSize = 1024*1024;
    private static final int MAX_DIRTY_BLOCKS = 30;
    
    private static final int MAX_BLOCK_LIMIT = 50;
    private static final int MIN_BLOCK_LIMIT = 10;
    
   
   
    IWriteBlockRunner blockRunner;
    
    public VfsBufferedFile( IVfsDir parent, String path, RemoteFSElem elem, RemoteFSApi remoteFSApi,  IWriteBlockRunner blockRunner )
    {
        super( parent, path, elem, remoteFSApi );
        blockMap = new HashMap<>(1);
        this.blockRunner =  blockRunner;
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
                    return (int)(o1.getLastTS() - o2.getLastTS());
                }
            });
            
            for (int i = 0; i < list.size(); i++ )
            {
                long offset = list.get(i).getOffset();
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
        if (block.getValidLen() == len && block.isComplete())
            return block.getData();

        // readpos in Block
        int offsetInBlock = (int)(offset - block.getOffset());            
        int readLen = len;
        // Calc readable len of data
        if (offsetInBlock + readLen > block.getValidLen())
        {
            readLen = block.getValidLen() - offsetInBlock;
        }
        // read data
        byte[] data = new byte[readLen];
        System.arraycopy( block.getData(), offsetInBlock, data, 0, readLen);
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
                        error( "Keine Daten bei Blockadresse " + offset + ": " + iOException);
                        throw iOException; 
                    }                    
                }

                // No Data on Server
                if (block == null)
                {
                    // Create new Block on Blockboundary
                    long blockOffset = getBlockOffset(realOffset);

                    block = blockRunner.getNewFullBlock(blockOffset, blockSize, 0);
//                    byte[] blockData = new byte[blockSize];
//                    block = new VfsBlock( blockOffset, blockSize, 0, blockData );    
                    blockMap.put( blockOffset, block);
                }
            }

            int offsetInBlock = (int)(realOffset - block.getOffset());   

            // Schreiben über Block hinaus?
            if (realLen > block.getLen() - offsetInBlock)
            {
                // Block ist zu klein?
                if (block.getLen() < blockSize)
                {
                    // Create new Block on Blockboundary
                    long blockOffset = getBlockOffset(offset);
                    byte[] blockData = new byte[blockSize];

                    // Kopiere bestehende Daten hinein
                    System.arraycopy( block.getData(), 0, blockData, 0, block.getLen());
                    //block = new VfsBlock( blockOffset, blockSize, 0, blockData );  
                    block = blockRunner.getNewFullBlock(blockOffset, blockSize, 0, blockData);
                    blockMap.put( blockOffset, block);
                }
                // Korrigiere tatsaechlich moegliche Laenge
                if (realLen > block.getLen() - offsetInBlock)
                {
                    realLen = block.getLen() - offsetInBlock;
                }
            }

            System.arraycopy( data, writtenLen, block.getData(), offsetInBlock, realLen);

            // Block hat neue Daten
            block.setDirtyWrite( true );

            // Neues Blockende prüfen
            if (block.getValidLen() < offsetInBlock + realLen)
            {
                block.setValidLen( offsetInBlock + realLen );
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
            
        }
        catch (PoolReadOnlyException | SQLException | PathResolveException ex)
        {
            throw new IOException( "Fehler bei close: " + ex.getMessage(), ex);
        }

        try
        {
            if (!blockRunner.flushAndWait())
            {
                throw new IOException("Fehler beim Übertragen der Daten");
            }
        }
        finally
        {
            // TODO: cache a la ehCache with blocks
            debug( "Clearing "+ blockMap.size() + " Blocks ");
            blockMap.clear();
            
            // Check wether Filesize has changed
            if (isUpdatedFileSize()) 
            {
                debug( "Korrigiere neue Länge von " + getNode().getName() + " -> " + getNode().getDataSize());
                try {
                    truncate(handleNo, getNode().getDataSize());
                }
                catch (SQLException | PoolReadOnlyException ex) {
                    throw new IOException("Fehler beim Setzen der neuen Größe von " + getNode().toString());
                }
            }
            super.close(handleNo);
        }                
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

        VfsBlock block = blockRunner.getNewFullBlock(blockOffset, data.length, data);
        //VfsBlock block = new VfsBlock( blockOffset, data.length, data);
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
                long diff = (o1.getLastTS() - o2.getLastTS());
                if (diff == 0)
                {
                    diff = (o1.getOffset()- o2.getOffset());
                }
                return Long.signum(diff);
            }
        });
        
       for (VfsBlock vfsBlock : list)
        {
            if (vfsBlock.isDirtyWrite())
            {
                debug( "Write Block " + vfsBlock.toString());
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
                long diff = (o1.getLastTS() - o2.getLastTS());
                if (diff == 0)
                {
                    diff = (o1.getOffset()- o2.getOffset());
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
        
        if (list.size() > MAX_BLOCK_LIMIT)
        {
            for (int i = 0; i < list.size() - MIN_BLOCK_LIMIT; i++ )
            {
                if (!list.get(i).isDirtyWrite())
                {
                    long offset = list.get(i).getOffset();
                    blockMap.remove(offset);                    
                }
            }
        }       
    }
    
    void write_block(long handleNo, VfsBlock vfsBlock) throws IOException, SQLException, PoolReadOnlyException, PathResolveException
    {
        blockRunner.write_block(handleNo, vfsBlock);
        
        if (getSize() < vfsBlock.getOffset() + vfsBlock.getValidLen())
        {
            getNode().setDataSize( vfsBlock.getOffset() + vfsBlock.getValidLen());
            setUpdatedFileSize(true);
        }        
    }
}
