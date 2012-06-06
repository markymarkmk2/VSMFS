

package de.dimm.vsm.fsutils;

import de.dimm.vsm.dokan.DokanVSMFS;
import de.dimm.vsm.fsutils.utils.OSValidator;
import de.dimm.vsm.fuse.FuseVSMFS;

public class UnmountFS
{
	public static void main(String[] args)
        {
		if (args.length == 0)
                {
                    System.err.println("Missing mountpoint");
                    System.exit(1);
                }
                if (OSValidator.isWindows())
                    DokanVSMFS.unmount(args[0]);
                else
                    FuseVSMFS.unmount(args[0]);
		System.exit(0);
	}
}
