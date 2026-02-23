import { Routes, Route } from "react-router-dom";
import Layout from "./components/Layout";
import Feed from "./pages/Feed";
import ExploreClubs from "./pages/ExploreClubs";
import MyClubs from "./pages/MyClubs";
import ClubDetail from "./pages/ClubDetail";
import LibraryPage from "./pages/Library";
import BookDetail from "./pages/BookDetail";
import Forum from "./pages/Forum";

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Feed />} />
        <Route path="/explore" element={<ExploreClubs />} />
        <Route path="/my-clubs" element={<MyClubs />} />
        <Route path="/club/:id" element={<ClubDetail />} />
        <Route path="/library" element={<LibraryPage />} />
        <Route path="/book/:id" element={<BookDetail />} />
        <Route path="/forum" element={<Forum />} />
      </Route>
    </Routes>
  );
}
