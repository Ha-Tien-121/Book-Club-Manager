import { useState } from "react";
import { Sparkles, SlidersHorizontal, TrendingUp } from "lucide-react";
import BookCard from "../components/BookCard";
import { books, genres } from "../data/mockData";

export default function Feed() {
  const [selectedGenres, setSelectedGenres] = useState([]);
  const [showFilters, setShowFilters] = useState(false);

  const toggleGenre = (genre) => {
    setSelectedGenres((prev) =>
      prev.includes(genre) ? prev.filter((g) => g !== genre) : [...prev, genre]
    );
  };

  const filtered =
    selectedGenres.length === 0
      ? books
      : books.filter((b) => b.genres.some((g) => selectedGenres.includes(g)));

  const trending = [...books].sort((a, b) => b.checkouts - a.checkouts).slice(0, 4);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
      <div className="mb-10">
        <h1 className="text-3xl sm:text-4xl font-serif font-bold text-warm-900">
          Discover your next read
        </h1>
        <p className="text-warm-500 mt-2 max-w-xl">
          Personalized book recommendations based on your preferences, reading
          history, and what Seattle is reading right now.
        </p>
      </div>

      {/* Trending */}
      <section className="mb-12">
        <div className="flex items-center gap-2 mb-5">
          <TrendingUp className="w-5 h-5 text-rose-400" />
          <h2 className="text-xl font-serif font-semibold text-warm-800">
            Trending in Seattle
          </h2>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          {trending.map((book) => (
            <BookCard key={book.id} book={book} compact />
          ))}
        </div>
      </section>

      {/* Recommendations */}
      <section>
        <div className="flex items-center justify-between mb-5">
          <div className="flex items-center gap-2">
            <Sparkles className="w-5 h-5 text-sage-500" />
            <h2 className="text-xl font-serif font-semibold text-warm-800">
              Recommended for you
            </h2>
          </div>
          <button
            onClick={() => setShowFilters(!showFilters)}
            className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm text-warm-600 hover:bg-warm-100 transition-colors border border-warm-200"
          >
            <SlidersHorizontal className="w-4 h-4" />
            Filters
          </button>
        </div>

        {showFilters && (
          <div className="mb-6 p-4 bg-warm-50 rounded-xl border border-warm-200">
            <p className="text-xs text-warm-400 uppercase tracking-wider mb-3">
              Genres
            </p>
            <div className="flex flex-wrap gap-2">
              {genres.map((g) => (
                <button
                  key={g}
                  onClick={() => toggleGenre(g)}
                  className={`px-3 py-1.5 rounded-full text-sm transition-all border ${
                    selectedGenres.includes(g)
                      ? "bg-sage-600 text-white border-sage-600"
                      : "bg-warm-50 text-warm-600 border-warm-300 hover:border-sage-400"
                  }`}
                >
                  {g}
                </button>
              ))}
            </div>
          </div>
        )}

        {filtered.length === 0 ? (
          <div className="text-center py-16 text-warm-400">
            <p className="text-lg">No books match your filters.</p>
            <button
              onClick={() => setSelectedGenres([])}
              className="mt-3 text-sage-600 hover:text-sage-700 text-sm underline"
            >
              Clear filters
            </button>
          </div>
        ) : (
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-5">
            {filtered.map((book) => (
              <BookCard key={book.id} book={book} />
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
