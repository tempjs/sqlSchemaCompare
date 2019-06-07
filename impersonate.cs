using Microsoft.Win32.SafeHandles;
using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;
using System.Security;
using System.Security.Permissions;
using System.Security.Principal;

namespace SqlDbSchemaCompare
{

    internal class ImpersonateIt : IDisposable
    {

        public const int LOGON32_PROVIDER_DEFAULT = 0;

        public const int LOGON32_LOGON_INTERACTIVE = 2;

        public const int LOGON32_LOGON_NETWORK = 3;

        public const int LOGON32_LOGON_BATCH = 4;

        public const int LOGON32_LOGON_SERVICE = 5;

        public const int LOGON32_LOGON_UNLOCK = 7;

        public const int LOGON32_LOGON_NETWORK_CLEARTEXT = 8;

        public const int LOGON32_LOGON_NEW_CREDENTIALS = 9;

        [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern bool LogonUser(string lpszUsername, string lpszDomain, string lpszPassword, int dwLogonType, int dwLogonProvider, out SafeTokenHandle phToken);

        [DllImport("kernel32.dll", CharSet = CharSet.Auto)]
        public static extern bool CloseHandle(IntPtr handle);

        WindowsIdentity windowsIdentity;
        WindowsImpersonationContext impersonationContext;
        SafeTokenHandle identityToken;

        [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
        internal void Impersonate(string userName, string password, string domain="FM")
        {
            if (string.IsNullOrWhiteSpace(userName))
                throw new ArgumentNullException("ÜserName is required to impersonate");

            if (string.IsNullOrWhiteSpace(password))
                throw new ArgumentNullException("User's Password is required to impersonate");


            Console.WriteLine("ImpersonateIt Executor utility is starting...");

            try
            {
                string lpszUsername = userName;
                string lpszDomain = "FM";

                if (userName.ToString().Contains("\\"))
                {
                    var userNameWithDomain = userName.Split('\\');
                    lpszDomain = userNameWithDomain.First();
                    lpszUsername = userNameWithDomain.Last();
                }
                string lpszPassword = password;

                //int result = 3600;

                Console.WriteLine("calling LogonUser.");

                bool flag = LogonUser(lpszUsername, lpszDomain, lpszPassword, 8, 0, out identityToken);
                // Console.WriteLine("LogonUser called succesfully and retuned value is " + flag);
                if (!flag)
                {
                    int lastWin32Error = Marshal.GetLastWin32Error();
                    Console.WriteLine("LogonUser failed with error code : {0}", lastWin32Error);
                    throw new Win32Exception(lastWin32Error);
                }
                Console.WriteLine("Value of Windows NT token: " + identityToken);

                Console.WriteLine("Before impersonation: " + WindowsIdentity.GetCurrent().Name);
                windowsIdentity = new WindowsIdentity(identityToken.DangerousGetHandle());

                impersonationContext = windowsIdentity.Impersonate();
                Console.WriteLine("Using " + WindowsIdentity.GetCurrent().Name + " Identity");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception occurred. " + ex.Message);
                throw ex;
            }
        }

        public void Dispose()
        {
            Console.WriteLine("Closeing Impersonation context");
            impersonationContext?.Dispose();
            windowsIdentity?.Dispose();
            identityToken.Dispose();
        }
    }
    public sealed class SafeTokenHandle : SafeHandleZeroOrMinusOneIsInvalid
    {
        private SafeTokenHandle()
            : base(ownsHandle: true)
        {
        }

        [DllImport("kernel32.dll")]
        [SuppressUnmanagedCodeSecurity]
        [ReliabilityContract(Consistency.WillNotCorruptState, Cer.Success)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool CloseHandle(IntPtr handle);

        protected override bool ReleaseHandle()
        {
            return CloseHandle(handle);
        }
    }
}
