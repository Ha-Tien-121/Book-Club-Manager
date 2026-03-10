import { Link } from "react-router-dom";
import { Plus, Calendar, BookOpen } from "lucide-react";
import { userClubs } from "../data/mockData";

function formatDate(dateStr) {
  return new Date(dateStr).toLocaleDateString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export default function MyClubs() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
      <div className="flex items-center justify-between mb-10">
        <div>
          <h1 className="text-3xl sm:text-4xl font-serif font-bold text-warm-900">
            My Clubs
          </h1>
          <p className="text-warm-500 mt-2">
            Your reading communities, all in one place.
          </p>
        </div>
        <Link
          to="/explore"
          className="flex items-center gap-2 px-4 py-2 bg-sage-600 text-white rounded-xl text-sm font-medium hover:bg-sage-700 transition-colors"
        >
          <Plus className="w-4 h-4" />
          Join a Club
        </Link>
      </div>

      {userClubs.length === 0 ? (
        <div className="text-center py-20">
          <p className="text-warm-400 text-lg mb-4">
            You haven't joined any clubs yet.
          </p>
          <Link
            to="/explore"
            className="inline-flex items-center gap-2 px-5 py-2.5 bg-sage-600 text-white rounded-xl text-sm font-medium hover:bg-sage-700 transition-colors"
          >
            <Plus className="w-4 h-4" />
            Explore Clubs
          </Link>
        </div>
      ) : (
        <div className="space-y-4">
          {userClubs.map((club) => (
            <Link
              key={club.id}
              to={`/club/${club.id}`}
              className="group flex flex-col sm:flex-row gap-5 p-5 bg-warm-50 rounded-2xl border border-warm-200 hover:border-warm-300 hover:shadow-md transition-all"
            >
              <div className="sm:w-48 h-32 sm:h-auto rounded-xl overflow-hidden bg-warm-100 shrink-0">
                <img
                  src={club.thumbnail}
                  alt={club.name}
                  className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-500"
                />
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <h3 className="font-serif text-lg font-semibold text-warm-900 group-hover:text-sage-700 transition-colors">
                      {club.name}
                    </h3>
                    <p className="text-sm text-warm-500 mt-1">
                      {club.members} members &middot; {club.location}
                    </p>
                  </div>
                  <span className="px-2.5 py-1 text-xs font-medium rounded-full bg-sage-50 text-sage-600 border border-sage-200 shrink-0">
                    {club.genre}
                  </span>
                </div>

                <div className="mt-4 flex flex-col sm:flex-row gap-3 sm:gap-6 text-sm text-warm-600">
                  <div className="flex items-center gap-2">
                    <BookOpen className="w-4 h-4 text-warm-400" />
                    <span>
                      Reading:{" "}
                      <span className="font-medium text-warm-700">
                        {club.currentBook?.title}
                      </span>
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    <Calendar className="w-4 h-4 text-warm-400" />
                    <span>Next: {formatDate(club.nextMeeting)}</span>
                  </div>
                </div>
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}
