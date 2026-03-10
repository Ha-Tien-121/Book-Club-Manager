import { Link } from "react-router-dom";
import { Star, Users, BookOpen } from "lucide-react";

export default function BookCard({ book, compact = false }) {
  return (
    <Link
      to={`/book/${book.id}`}
      className="group bg-warm-50 rounded-2xl overflow-hidden border border-warm-200 hover:border-warm-300 hover:shadow-lg transition-all duration-300"
    >
      <div className="aspect-[2/3] overflow-hidden bg-warm-100">
        <img
          src={book.cover}
          alt={book.title}
          className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-500"
          loading="lazy"
        />
      </div>
      <div className="p-4">
        <h3 className="font-serif font-semibold text-warm-900 leading-snug line-clamp-2 group-hover:text-sage-700 transition-colors">
          {book.title}
        </h3>
        <p className="text-sm text-warm-500 mt-1">{book.author}</p>

        {!compact && (
          <>
            <div className="flex items-center gap-3 mt-3 text-xs text-warm-500">
              <span className="flex items-center gap-1">
                <Star className="w-3.5 h-3.5 text-amber-500 fill-amber-500" />
                {book.rating}
              </span>
              <span className="flex items-center gap-1">
                <Users className="w-3.5 h-3.5" />
                {book.clubsReading} clubs
              </span>
              {book.splAvailable && (
                <span className="flex items-center gap-1 text-sage-600">
                  <BookOpen className="w-3.5 h-3.5" />
                  SPL
                </span>
              )}
            </div>
            <div className="flex flex-wrap gap-1.5 mt-3">
              {book.genres.slice(0, 2).map((g) => (
                <span
                  key={g}
                  className="px-2 py-0.5 text-xs rounded-full bg-sage-50 text-sage-600 border border-sage-200"
                >
                  {g}
                </span>
              ))}
            </div>
          </>
        )}
      </div>
    </Link>
  );
}
