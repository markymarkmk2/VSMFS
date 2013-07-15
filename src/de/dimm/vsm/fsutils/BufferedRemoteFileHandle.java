/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsutils;

import de.dimm.vsm.Exceptions.PoolReadOnlyException;
import de.dimm.vsm.Utilities.MaxSizeHashMap;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;


/**
 *
 * @author Administrator
 */
public class BufferedRemoteFileHandle extends RemoteFileHandle
{

    byte[] writeBuff;
    long writeBuffOffset;
    int writeBuffPos;

    int readBlocksize;
    
    
    class DataElem
    {
        long offset;
        byte[] data;

        public DataElem( long offset, byte[] data )
        {
            this.offset = offset;
            this.data = data;
        }
        
        
    }
   
    
    MaxSizeHashMap<Long, DataElem> readBuffStack = new MaxSizeHashMap<>(30);
    
    public BufferedRemoteFileHandle( RemoteStoragePoolHandler remoteFSApi, long idx, int readBlocksize, int writeBlocksize )
    {
        super(remoteFSApi, idx);
        this.readBlocksize = readBlocksize;
        writeBuff = new byte[writeBlocksize];
        writeBuffOffset = 0;
        writeBuffPos = 0;        
    }

    @Override
    public void writeFile( byte[] b, int length, long offset ) throws IOException, SQLException, PoolReadOnlyException
    {
        l("buffwriteFile");
        
        // First check if we are overwriting something inside our writebuffer
        if (offset >= writeBuffOffset && offset + length <= writeBuffOffset + writeBuffPos)
        {
            int realOffset = (int)(offset - writeBuffOffset); 
            System.arraycopy(b, 0, writeBuff, realOffset, length);
            // Nothing more to do -> leave
            return;
        }
        
        // Needs flush? (Start doenst match or Length is too big))?
        if (writeBuffOffset + writeBuffPos != offset || writeBuffPos + length > writeBuff.length)
        {
            if (writeBuffPos > 0)
            {
                remoteFSApi.writeFile( serverFhIdx, writeBuff, writeBuffPos, writeBuffOffset );
                writeBuffPos = 0;
            }

            // Set new Offset for Buffer
            writeBuffOffset = offset;
        }

        // Too large ?
        if (length > writeBuff.length)
        {
            remoteFSApi.writeFile( serverFhIdx, b, length, offset );
            return;
        }

        // Add to buffer
        System.arraycopy(b, 0, writeBuff, writeBuffPos, length);
        writeBuffPos += length;        
    }

    @Override
    public void close() throws IOException
    {
        l("close");
        try
        {
            if (writeBuffPos > 0)
            {
                remoteFSApi.writeFile(serverFhIdx, writeBuff, writeBuffPos, writeBuffOffset);
                writeBuffPos = 0;
            }
        }

        catch (SQLException sQLException)
        {
            throw new IOException("Sql-Fehler beim Flush", sQLException);
        }
        catch (PoolReadOnlyException poolReadOnlyException)
        {
            throw new IOException("RDONLY-Fehler beim Flush", poolReadOnlyException);
        }
        remoteFSApi.close( serverFhIdx );
        
        readBuffStack.clear();
    }

    @Override
    public byte[] read( int length, long offset ) throws IOException
    {
        // IST IN WriteBuffer ?
        if (offset >= writeBuffOffset && offset < writeBuffPos)
        {
            int rlen = length;
            int blockOffset = (int)(offset - writeBuffOffset);
            if (blockOffset + rlen > writeBuffPos)
            {
                rlen = writeBuffPos - blockOffset;
            }
            byte[] tmp = new byte[rlen];
            System.arraycopy(writeBuff, blockOffset, tmp, 0, rlen);
            return tmp;                    
        }
        
        //l("buffread len:" + length + " off:" + offset);
        // WENN WIR SCHON OPTIMAL ABFRAGEN, NICHTS OPTIMIEREN
        if (length == readBlocksize && offset % length == 0)
            return super.read(length, offset); 
        
        int restLen = length;
        

        byte[] ret = new byte[length];
        
        while (restLen > 0)
        {            
            long realOffset = (offset / readBlocksize) * readBlocksize;
            
            // Remove and put to Map to keep newest and most used in Map
            DataElem elem = this.readBuffStack.remove(realOffset);
            if (elem == null)
            {
                byte[] tmp = super.read(readBlocksize, realOffset); 
                elem = new DataElem(realOffset, tmp);
            }
            else
            {
                //l("Cachehit for len:" + elem.data.length + " off:" + elem.offset + " size " + readBuffStack.size());
            }
            byte[] readBuff = elem.data;

            readBuffStack.put(elem.offset, elem);
            
            // READ OFFS INSIDE BLOCK
            int offsetInBlock = (int)(offset - realOffset);
            // COPY TO TARGET
            int copyLen = restLen;
            
            // END OF COPYBLOCK OUTSIDE OF OUR CURRENT BLOCK
            if (copyLen  + offsetInBlock > readBuff.length)
                copyLen = readBuff.length - offsetInBlock;
            
            // ARE WE READING BEYOND EOF?
            if (copyLen < 0)
            {
                ret = new byte[0];
                break; //>>>>>>>                
            }
            
            l("copy src-off: " + offsetInBlock + " trg-off:" + (length - restLen) + " len:" + copyLen);
            // Got Prematore end / Read over end of file
            if (copyLen  == 0)
            {
                byte[] tmp = ret;
                ret = new byte[length - restLen];
                System.arraycopy(tmp, 0, ret, 0, ret.length);
                break;
            }
            // COPY DATA
            l("copy src-off: " + offsetInBlock + " trg-off:" + (length - restLen) + " len:" + copyLen);
            try
            {
                System.arraycopy(readBuff, offsetInBlock, ret, length - restLen, copyLen);                
            }
            catch (Exception e)
            {
                l("kack");
            }
            
            // ADJUST OFFSET 
            restLen -= copyLen;
            offset += copyLen;
        }
        return ret;
    }
}