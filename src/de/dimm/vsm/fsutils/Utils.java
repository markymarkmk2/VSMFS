package de.dimm.vsm.fsutils;


public class Utils
{

    public static String trimTailBackSlash( String path )
    {
        if (path.endsWith("\\"))
        {
            return path.substring(0, path.length() - 1);
        }
        else
        {
            return path;
        }
    }

    // This is not correct.
    public static String toShortName( String fileName )
    {
        String base = fileName;
        if (base.length() > 8)
        {
            base = base.substring(8);
        }

        String ext = null;
        int suffidx = fileName.lastIndexOf('.');
        if (suffidx > 0 && suffidx < fileName.length() - 1)
        {
            ext = fileName.substring(suffidx + 1);
        }
        if (ext != null)
        {
            if (ext.length() > 3)
            {
                ext = ext.substring(3);
            }
            if (ext.length() > 0)
            {
                ext = "." + ext;
            }
        }

        return base + ext;
    }
}
