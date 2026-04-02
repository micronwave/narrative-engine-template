import type { Metadata } from "next";
import { IBM_Plex_Sans, IBM_Plex_Mono } from "next/font/google";
import "./globals.css";
import { AuthProvider } from "@/contexts/AuthContext";
import { AlertProvider } from "@/contexts/AlertContext";
import { WatchlistProvider } from "@/contexts/WatchlistContext";
import NavBar from "@/components/NavBar";
import QueryProvider from "@/components/QueryProvider";

const ibmPlexSans = IBM_Plex_Sans({
  subsets: ["latin"],
  display: "swap",
  variable: "--font-ibm-plex-sans",
  weight: ["400", "500", "600", "700"],
});

const ibmPlexMono = IBM_Plex_Mono({
  subsets: ["latin"],
  display: "swap",
  variable: "--font-ibm-plex-mono",
  weight: ["400", "500", "600", "700"],
});

export const metadata: Metadata = {
  title: "Narrative Intelligence",
  description:
    "Live financial narrative signal radar — powered by Narrative Engine",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className={`${ibmPlexSans.variable} ${ibmPlexMono.variable}`}>
      <body className={`${ibmPlexSans.className} antialiased`}>
        <QueryProvider>
          <AuthProvider>
            <AlertProvider>
              <WatchlistProvider>
                <NavBar />
                <main className="min-h-screen md:ml-16 pb-20 md:pb-0">
                  {children}
                </main>
              </WatchlistProvider>
            </AlertProvider>
          </AuthProvider>
        </QueryProvider>
      </body>
    </html>
  );
}
