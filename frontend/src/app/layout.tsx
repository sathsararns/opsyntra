import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Opsyntra — Customer intelligence",
  description: "Understand customer churn, explore your models, and make confident retention decisions with Opsyntra.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="en"><body>{children}</body></html>;
}
