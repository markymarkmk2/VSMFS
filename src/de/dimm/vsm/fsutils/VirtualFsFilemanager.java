/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.fsutils;

import de.dimm.vsm.VSMFSLogger;
import de.dimm.vsm.net.RemoteFSElem;
import de.dimm.vsm.vfs.IBufferedEventProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Administrator
 */
public class VirtualFsFilemanager
{
    Map<String,IVirtualFSFile> fileList = new ConcurrentHashMap<>();    
    
    static VirtualFsFilemanager singleton;

    public static VirtualFsFilemanager getSingleton()
    {
        if (singleton == null)
            singleton = new VirtualFsFilemanager();
        
        return singleton;
    }
    
    public void addFile(String path, IVirtualFSFile file)
    {
        fileList.put(path, file);
        VSMFSLogger.getLog().debug("VirtualFsFilemanager add " + path + " size " + fileList.size());
    }
    
    public IVirtualFSFile getFile(String path)
    {
        return fileList.get(path);
    }
    
    public IVirtualFSFile removeFile(IBufferedEventProcessor processor, String path)
    {
        IVirtualFSFile ret =  fileList.remove(path);
        VSMFSLogger.getLog().debug("VirtualFsFilemanager rem " + path +" size " + fileList.size());        
        processor.removeEntry( ret);
        return ret;
    }    

    public IVirtualFSFile createFile( IBufferedEventProcessor processor, RemoteFSElem fseNode )
    {                           
        IVirtualFSFile file = processor.createFile(fseNode);        
        return file;
    }


   
    
    void idle(IBufferedEventProcessor processor)
    {
   
        processor.init();
        
        processor.idle();
    }

    public List<RemoteFSElem> get_child_nodes( String win_to_sys_path )
    {
        List<RemoteFSElem> list = new ArrayList<>();
        
        for (Map.Entry<String, IVirtualFSFile> entry : fileList.entrySet())
        {
            String fullPath = entry.getKey();
            if (fullPath.startsWith(win_to_sys_path))
            {
                String fileName = fullPath.substring(win_to_sys_path.length());
                if (!fileName.contains("/"))
                {
                    IVirtualFSFile iVirtualFSFile = entry.getValue();                    
                    list.add(iVirtualFSFile.getElem());
                }
            }            
        }
        return list;
    }
}
