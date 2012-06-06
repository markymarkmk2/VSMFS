package de.dimm.vsm.fsutils.utils;

public class FileUtils
{

    public static int parseFilePermissions( String mode )
    {
        if (mode.equalsIgnoreCase("---"))
        {
            return 0;
        }
        if (mode.equalsIgnoreCase("--x"))
        {
            return 1;
        }
        if (mode.equalsIgnoreCase("-w-"))
        {
            return 2;
        }
        if (mode.equalsIgnoreCase("-wx"))
        {
            return 3;
        }
        if (mode.equalsIgnoreCase("r--"))
        {
            return 4;
        }
        if (mode.equalsIgnoreCase("r-x"))
        {
            return 5;
        }
        if (mode.equalsIgnoreCase("rw-"))
        {
            return 6;
        }
        if (mode.equalsIgnoreCase("rwx"))
        {
            return 7;
        }
        else
        {
            return 0;
        }

    }

    public static String parseFilePermissions( int mode )
    {
        String mm = Integer.toString(mode);
        char[] c = mm.toCharArray();
        StringBuffer b = new StringBuffer();
        for (int i = 0; i < c.length; i++)
        {
            System.out.println("parsing " + c[i]);
            if (c[i] == '0')
            {
                b.append("---");
            }
            if (c[i] == '1')
            {
                b.append("--x");
            }
            if (c[i] == '2')
            {
                b.append("-w-");
            }
            if (c[i] == '3')
            {
                b.append("-wx");
            }
            if (c[i] == '4')
            {
                b.append("r--");
            }
            if (c[i] == '5')
            {
                b.append("r-x");
            }
            if (c[i] == '6')
            {
                b.append("rw-");
            }
            if (c[i] == '7')
            {
                b.append("rwx");
            }
        }
        return b.toString();

    }
    /*
    public static void setPermissions(String path, int permissions)
    throws IOException {
    HashSet<PosixFilePermission> set = new HashSet<PosixFilePermission>();
    char[] smak = parseFilePermissions(permissions).toCharArray();
    if (smak[0] == 'r')
    set.add(PosixFilePermission.OWNER_READ);
    if (smak[1] == 'w')
    set.add(PosixFilePermission.OWNER_WRITE);
    if (smak[2] == 'x')
    set.add(PosixFilePermission.OWNER_EXECUTE);
    if (smak[3] == 'r')
    set.add(PosixFilePermission.GROUP_READ);
    if (smak[4] == 'w')
    set.add(PosixFilePermission.GROUP_WRITE);
    if (smak[5] == 'x')
    set.add(PosixFilePermission.GROUP_EXECUTE);
    if (smak[6] == 'r')
    set.add(PosixFilePermission.OTHERS_READ);
    if (smak[7] == 'w')
    set.add(PosixFilePermission.OTHERS_WRITE);
    if (smak[8] == 'x')
    set.add(PosixFilePermission.OTHERS_EXECUTE);
    File f = new File(path);
    Path p = f.toPath();
    PosixFileAttributeView view = (PosixFileAttributeView) p
    .getFileAttributeView(PosixFileAttributeView.class);
    view.setPermissions(set);
    }

    public static int getFilePermissions(String path) throws IOException {
    File f = new File(path);
    Path p = f.toPath();
    PosixFileAttributeView view = (PosixFileAttributeView) p
    .getFileAttributeView(PosixFileAttributeView.class);

    PosixFileAttributes attrs1 = (PosixFileAttributes) view
    .readAttributes();
    String mode = PosixFilePermissions.toString(attrs1.permissions());
    String mm = parseFilePermissions(mode.substring(0, 3)) + ""
    + parseFilePermissions(mode.substring(3, 6)) + ""
    + parseFilePermissions(mode.substring(6, 9));
    return Integer.parseInt(mm);
    }
     */
}
