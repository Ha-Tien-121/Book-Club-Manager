import { useState } from "react";
import { Search, MapPin, SlidersHorizontal } from "lucide-react";
import ClubCard from "../components/ClubCard";
import { clubs, genres, neighborhoods } from "../data/mockData";

export default function ExploreClubs() {
  const [search, setSearch] = useState("");
  const [selectedGenre, setSelectedGenre] = useState("");
  const [selectedNeighborhood, setSelectedNeighborhood] = useState("");
  const [showFilters, setShowFilters] = useState(false);

  const filtered = clubs.filter((club) => {
    const matchesSearch =
      !search ||
      club.name.toLowerCase().includes(search.toLowerCase()) ||
      club.description.toLowerCase().includes(search.toLowerCase());
    const matchesGenre = !selectedGenre || club.genre === selectedGenre;
    const matchesLocation =
      !selectedNeighborhood ||
      club.location.toLowerCase().includes(selectedNeighborhood.toLowerCase());
    return matchesSearch && matchesGenre && matchesLocation;
  });

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
      <div className="mb-10">
        <h1 className="text-3xl sm:text-4xl font-serif font-bold text-warm-900">
          Find your community
        </h1>
        <p className="text-warm-500 mt-2 max-w-xl">
          Discover active book clubs and reading events in the Seattle area that
          match your interests and schedule.
        </p>
      </div>

      {/* Search & Filters */}
      <div className="flex flex-col sm:flex-row gap-3 mb-6">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-warm-400" />
          <input
            type="text"
            placeholder="Search clubs..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full pl-10 pr-4 py-2.5 rounded-xl bg-warm-50 border border-warm-200 text-warm-800 placeholder:text-warm-400 focus:outline-none focus:ring-2 focus:ring-sage-300 focus:border-sage-300 text-sm"
          />
        </div>
        <button
          onClick={() => setShowFilters(!showFilters)}
          className="flex items-center justify-center gap-2 px-4 py-2.5 rounded-xl border border-warm-200 text-sm text-warm-600 hover:bg-warm-100 transition-colors"
        >
          <SlidersHorizontal className="w-4 h-4" />
          Filters
        </button>
      </div>

      {showFilters && (
        <div className="mb-8 p-5 bg-warm-50 rounded-xl border border-warm-200 grid sm:grid-cols-2 gap-4">
          <div>
            <label className="text-xs text-warm-400 uppercase tracking-wider block mb-2">
              Genre / Topic
            </label>
            <select
              value={selectedGenre}
              onChange={(e) => setSelectedGenre(e.target.value)}
              className="w-full px-3 py-2 rounded-lg bg-white border border-warm-200 text-sm text-warm-700 focus:outline-none focus:ring-2 focus:ring-sage-300"
            >
              <option value="">All genres</option>
              {genres.map((g) => (
                <option key={g} value={g}>
                  {g}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="text-xs text-warm-400 uppercase tracking-wider block mb-2">
              <MapPin className="w-3 h-3 inline mr-1" />
              Neighborhood
            </label>
            <select
              value={selectedNeighborhood}
              onChange={(e) => setSelectedNeighborhood(e.target.value)}
              className="w-full px-3 py-2 rounded-lg bg-white border border-warm-200 text-sm text-warm-700 focus:outline-none focus:ring-2 focus:ring-sage-300"
            >
              <option value="">All neighborhoods</option>
              {neighborhoods.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </div>
        </div>
      )}

      {filtered.length === 0 ? (
        <div className="text-center py-16 text-warm-400">
          <p className="text-lg">No clubs match your search.</p>
          <button
            onClick={() => {
              setSearch("");
              setSelectedGenre("");
              setSelectedNeighborhood("");
            }}
            className="mt-3 text-sage-600 hover:text-sage-700 text-sm underline"
          >
            Clear all filters
          </button>
        </div>
      ) : (
        <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-6">
          {filtered.map((club) => (
            <ClubCard key={club.id} club={club} />
          ))}
        </div>
      )}
    </div>
  );
}
