/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.vfs;

/**
 *
 * @author Administrator
 */
public class VfsBlock
{
    private long offset;
    private int len;
    private byte[] data;
    private boolean dirtyWrite;
    private int validLen;
    private long lastTS;
    private String hash;
    

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

    @Override
    public String toString()
    {
        return "N: " + Long.toString(offset/(1024*1024)) + " L: " + len + " O: " + offset + " H: " + hash; 
    }
    
    public void setOffsetLen(long offset, int len)
    {
        this.offset = offset;
        this.len = len;
        this.validLen = len;
        hash = null;
        touchTS();
        
    }
    public void setOffsetLenValidLen(long offset, int len, int validLen)
    {
        this.offset = offset;
        this.len = len;    
        this.validLen = validLen;
        hash = null;
        touchTS();
    }
         
    public void setOffsetLenData(long offset, int len, byte[]data)
    {
        this.offset = offset;
        this.len = len;    
        this.validLen = len;
        this.data = data;
        hash = null;
        touchTS();
    }
         
    public void setOffsetLenValidLenData(long offset, int len, int valid_len, byte[]data)
    {
        this.offset = offset;
        this.len = len;    
        this.validLen = valid_len;
        this.data = data;
        hash = null;
        touchTS();
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

    void setHash( String encodeUrlsafe )
    {
        hash = encodeUrlsafe;
    }

    public String getHash()
    {
        return hash;
    }

    public byte[] getData()
    {
        return data;
    }

    public long getLastTS()
    {
        return lastTS;
    }

    public int getLen()
    {
        return len;
    }

    public long getOffset()
    {
        return offset;
    }

    public int getValidLen()
    {
        return validLen;
    }

    public void setValidLen( int validLen )
    {
        this.validLen = validLen;
    }
    
    
}
